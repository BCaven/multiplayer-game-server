"""
GameServer.py
"""

import socket
import logging
import json
import sys
import os
from time import time
import select
import argparse
from typing import Type, Tuple
from GameEngine import Engine, Game

END_SEQ = ("END_OF_MESSAGE", "ALT_TERMINATION")
READ_ONLY = ( select.POLLIN |
              select.POLLPRI |
              select.POLLHUP |
              select.POLLERR )
ENCODING = 'utf-8'

class GameServer:
    """
    Server that runs the game based on client requests
    """
    def __init__(self, 
                 host: str, 
                 id: int = 0,
                 port: int = 0, 
                 log: str ='game.log', 
                 checkpoint: str = 'game.ckpt', 
                 nameserver: str = 'catalog.cse.nd.edu:9097', 
                 project_name: str = "game-server",
                 nameserver_broadcast_time: int = 600,
                 stdsrc = None,
                 engine_type: Type[Engine] = Game,
                 info_log_file: str = '',
                 owner: str = 'me',
                 server_type: str = 'game_server',
                 broadcast_with_udp: bool = False):
        """
        Create the game server

        by default, 
            the server listens to all ports
            the uses game.log and game.ckpt for persistence
            the server uses the nameserver catalog.cse.nd.edu:9097

        the room servers,
            pick a random port
            use room{index}.log/ckpt for persistence
            use the cluster server as their nameserver
        """
        self.broadcast_with_udp = broadcast_with_udp
        self.stdsrc = stdsrc
        self.log = logging.getLogger(__name__)
        if info_log_file:
            # add the file handler to self.log
            file_handler = logging.FileHandler(info_log_file)
            # right now just sending everything, if we have extra time
            # we could make it so only specified levels get logged
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            self.log.addHandler(file_handler)
        # keep track of this server's stats
        self.lifetime_stats: dict = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((host, port))
        hname = socket.gethostname()
        self.host = socket.gethostbyname(hname)
        self.port = self.sock.getsockname()[1]
        self.log.info("server listening on %s:%s", self.host, self.sock.getsockname()[1])
        # nameserver
        self.nameserver = nameserver
        self.project_name = project_name
        self.id = id
        # if this is negative, do not broadcast
        # a.k.a. broadcast time is negative when we have multiple servers in a cluster
        self.nameserver_broadcast_interval = nameserver_broadcast_time
        self.nameserver_message = {
            "type": server_type,
            "owner": owner,
            "port": self.port,
            "project": self.project_name,
        }
        # log and checkpoint
        # the checkpoint does not need to be open all the time
        # NOTE: currently opening log in append-and-read mode
        self.log_file = open(log, 'a+', encoding=ENCODING)
        self.log_file_name = log
        self.ckpt_name = checkpoint
        self.log_length = 0
        # game engine
        if engine_type == Game:
            self.log.info("Starting server with game engine")
        elif engine_type == Engine:
            raise NotImplementedError("Cannot pass abstract class as engine type")
        else:
            self.log.info("Starting server with cluster engine")

        # make a new instance of our engine class
        self.engine = engine_type(
            host=self.host, 
            port=self.port,
            use_udp = broadcast_with_udp
            )
        # list of connected clients
        # useful for the broadcast messages so we do not broadcast
        # "corpses" of players when they arent active in the room
        # but we still want to keep track of where it is
        # keeping track of it with self.connections
        # record all connected addresses and map connections to ids
        # TODO: check if there is a better way to do this
        self.connections: dict = {}
        self.socket_id_map: dict = {}

        # load from log/ckpt
        self._load_server()
        self._load_from_log()        

        # keep track of how many updates we have sent
        self.frames = 0

        # broadcast that the server is running on startup
        if self.nameserver_broadcast_interval > 0:
            self._broadcast(self.nameserver_message)

    def _broadcast(self, message):
        """
        Broadcast a message to the nameserver

        In general, there are two cases:
         1) the name server is the nd name server and we are
            telling it where we are
         2) we are a room and our name server is the server cluster,
            the message we are probably sending is 'shutdown_room'
            but we could theoretically send whatever
        """
        # send to nameserver
        try:
            nameserver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server = self.nameserver.split(':')
            ns = (server[0], int(server[1]))
            nameserver_socket.sendto(json.dumps(message).encode(ENCODING), ns)
            self.log.debug("broadcast sent to %s: %s", self.nameserver, message)
            # close the nameserver socket once the broadcast is done
            nameserver_socket.close()
        except Exception as e:
            self.log.error("When broadcasting to nameserver, caught message %s", e)
    
    def _broadcast_current_room_state(self):
        """
        Broadcast the current game state to everyone connected to the room

        If we were LAN only we could use multicast groups but sadly we are not
        so we have to keep track of everyone in the room
        fortunately we already have access to every connected socket, so we can send the message
        to all of them
        """
        # doing it the manual way for now
        if not isinstance(self.engine, Game):
            self.log.warning("Trying to call broadcast_current_room_state on invalid engine type %s", type(self.engine))
            return
        try:
            broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.frames += 1
            self.log.debug("The room: %s", self.engine.room)
            room_items = {val['name']: key for key, val in self.engine.room.items()}

            alive_clients = {c: self.engine.clients[c] for c in self.socket_id_map.values()}
            self.log.debug("room items before adding the clients: %s", room_items)
            if alive_clients:
                room_items |= alive_clients
            message = {
                # we need all of the things in the room
                'room': room_items,
                'frame': self.frames,
                'room_id': self.id
            }
            message_bytes = (json.dumps(message) + END_SEQ[0]).encode(ENCODING)
            self.log.info("Broadcasting room state to %s connections", len(self.connections))
            self.log.debug("Broadcasting the current state: %s", message_bytes)
            self.log.debug("Broadcasting to: %s", self.connections)
            # UDP is blocked on some networks so it is not used by default
            for _, str_addr in self.connections.items():
                split_addr = str_addr.split(':')
                addr = (split_addr[0], int(split_addr[1]))
                broadcast_socket.sendto(message_bytes, addr)
        except Exception as e:
            self.log.warning("Caught exception when trying to broadcast room state: %s", e)
    
    def _send_shutdown_message(self):
        """
        Send the shutdown message to the cluster server

        Message should look like:
        {
        'method': 'shutdown_room',
        'client': this room's id
        }
        """
        message = {
            'method': 'shutdown_room',
            'client': self.id
        }
        # connect to the server cluster
        self.log.info("Room %s shutdown process: connecting to cluster", self.id)
        server = self.nameserver.split(':')
        ns = (server[0], int(server[1]))
        cluster_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cluster_sock.connect(ns)
        self.log.info("Room %s shutdown process: sending shutdown message", self.id)
        cluster_sock.sendall((json.dumps(message) + END_SEQ[0]).encode('utf-8'))
        self.log.info("Room %s shutdown process: shutdown message sent", self.id)

    def _load_server(self):
        """
        Load the existing server state
        
        if the engine is Game:
            checkpoint will contain
            {room}
            {clients}
        
        if the engine is a Cluster:
            dont save anything
        """
        if not isinstance(self.engine, Game):
            self.log.info("Only Game servers load from checkpoints")
            return
        new_clients = {}
        new_room = {}
        try:
            self.log.info("Loading from checkpoint: %s", self.ckpt_name)
            with open(self.ckpt_name, 'r', encoding=ENCODING) as ckpt_file:
                self.log.debug("Reading lines from checkpoint")
                ckpt_lines = ckpt_file.readlines()
                ckpt_room_str = '{}'
                ckpt_client_str = '{}'
                if len(ckpt_lines) != 2:
                    self.log.error("malformed ckpt file when loading the sheet, %s", ckpt_lines)
                else:
                    self.log.debug("loading sheet from checkpoint")
                    ckpt_room_str = ckpt_lines[0]
                    ckpt_client_str = ckpt_lines[1]
                self.log.info("Trying to read room from log: %s", ckpt_room_str)
                self.log.info("Trying to read clients from log: %s", ckpt_client_str)
                new_room = json.loads(ckpt_room_str)
                new_clients = json.loads(ckpt_client_str)
        except FileNotFoundError:
            self.log.info("No checkpoint found, loading an empty room")
        
        if isinstance(new_room, dict) and isinstance(new_clients, dict):
            # TODO: make sure the room actually loads integers properly
            if new_room:
                self.log.info("Checkpoint read %s for the room so it is overwriting the randomly generated one", new_room)
                self.engine.room = new_room
            if new_clients:
                self.engine.clients = new_clients
        else:
            self.log.warning("tried to load malformed room: %s", new_room)
            # the engine should generate a random configuration on startup
            # so if we read a malformed ckpt it doesnt matter because we already
            # have a random config
         
    def _load_from_log(self):
        """
        Need to load from the log *after* the operations have been defined for this engine
        """
        if not isinstance(self.engine, Game):
            self.log.info("Only Game servers load from logs")
            return
        self.log.debug("loading from log on startup")
        # go to the start of the log
        self.log_file.seek(0)
        log_lines = self.log_file.readlines()
        # self.log.debug("log: %s", log_lines)
        for line in log_lines:
            # we already know the operations are safe because we checked them when putting them into the log
            # technically someone could hand modify the log... so I guess we have to check them again
            try:
                jdata = json.loads(line)
                self._parse_command(jdata)
            except Exception as e:
                # ah shit, its an invalid json in our log
                self.log.error("failed to parse json: %s", e)
                self.log.warning("dropped line %s", line)
        # and go back to the end of the file so we are still in order
        self.log_file.seek(0, 2)
  
    def _update_ckpt(self):
        """
        Open the checkpoint and write to it
        
        Steps:
        1. write current room to new file called name.ckpt.new
            (or whatever name was used with a '.new' on the end)
        2. move name.ckpt.new to name.ckpt
        3. truncate log 

        but what should be stored in the checkpoint?
        since the checkpoints will need to have different things,
        we could just have the engine specify what dictionaries get dumped
        into the file and read them back out when it is time to load from
        a checkpoint.
        """
        if not isinstance(self.engine, Game):
            self.log.warning("Only Game servers have checkpoints")
            return
        self.log.info("Creating new checkpoint")
        with open(self.ckpt_name + '.new', 'w', encoding=ENCODING) as ckpt:
            # write room
            ckpt.write(json.dumps(self.engine.room) + '\n')
            # write the clients
            ckpt.write(json.dumps(self.engine.clients) + '\n')
        
        os.replace(self.ckpt_name + '.new', self.ckpt_name)
        self._truncate_log()

    def _truncate_log(self):
        """
        Truncate the log

        This should only happen when the checkpoint is up to date
        NOTE: we should not close the log
        """
        self.log.info("trucating log")
        self.log_file.truncate(0)
        self.log_length = 0

    def _recv_all(self, sock, n):
        """
        Call recv until you have gotten all you are going to get from one message

        Recv until you get a string that ends with the END character

        """
        data = ""
        while new_data := sock.recv(n).decode('utf-8'):
            for ending in END_SEQ:
                if new_data[-1 * len(ending):] == ending:
                    return data + new_data[:-1 * len(ending)]
            data += new_data
        # if we get here it means the client closed the connection without sending the end of message sequence
        return data

    def _parse_command(self, incoming_command: dict, read_from_log = False):
        """
        Use self.engine.command_map

        We should be getting an enum from the client
        and this function should have two items
        {
        'method': int
        'client': client
        }
        clients are dictionaries that have
        {
        'id': int
        'current_room': int
        }
        since this function is only called internally, we do not need to check for malformed incomming commands
        """
        REQUIRED_ITEMS = ('method', 'client')
        return_message: dict = {'error': 'no command to parse'}
        
        if not all(item in incoming_command for item in REQUIRED_ITEMS):
            self.log.warning("malformed incoming command: %s", incoming_command)
            return_message = {'error': 'malformed incomming command'} 
        else:
            # we also want to check if 'broadcast_addr' is in the message and update the client's
            # broadcast addr
            if 'broadcast_addr' in incoming_command: 
                # add the broadcast addr to the dict
                self.connections[incoming_command['client']] = incoming_command['broadcast_addr']
            if incoming_command['method'] not in self.engine.command_map:
                return_message = {'error': f'method {incoming_command["method"]} does not exist for engine: {type(self.engine)}'}
            else:
                if incoming_command['method'] == 'get_room':
                    alive_clients = {c: self.engine.clients[c] for c in self.socket_id_map.values()}
                    self.log.debug("Currently alive clients: %s", alive_clients)
                    return_message = self.engine.command_map[incoming_command['method']](alive_clients)
                    self.log.debug("The room we are sending back: %s", return_message)
                else:
                    return_message = self.engine.command_map[incoming_command['method']](incoming_command['client'])
                # check if we needed to log that message
                if not read_from_log and isinstance(self.engine, Game):
                    # add the command to the log
                    self.log_file.write(json.dumps(incoming_command) + '\n')
                    self.log_file.flush()
                    os.fsync(self.log_file.fileno())
                    self.log_length += 1

                    if self.log_length > 100:
                        self._update_ckpt()
        # log errors
        if "error" in return_message:
            if "errors" in self.lifetime_stats:
                self.lifetime_stats["errors"] += 1
            else:
                self.lifetime_stats["errors"] = 1
        return return_message

    def _listen_to_client(self, connection, addr=None) -> Tuple[bool, dict]:
        """
        Listen to what the client has to say
        
        clients should be sending something that looks like:
        {
        'method': int,
        'client': client_id
        }
        TODO: decide if we are keeping a map of client ids and known ip addresses

        Returns True if connection is still open
        Returns False if connection is closed
        """

        #with connection:
        if addr:
            self.log.debug("Listening to %s", addr)
        try:
            data = self._recv_all(connection, 1024)
        except Exception as e:
            self.log.error("failed to read from client: %s", e)
            return False, {}
        self.log.debug("Recieved %s", data)
        if not data:
            self.log.info("Connection did not send data, no longer attempting to receive")
            return False, {}
        
        parse_error = False
        try:
            jdata = json.loads(data)
        except:
            self.log.error("failed to parse %s", data)
            parse_error = True
        if parse_error:
            try:
                connection.sendall(("{'error': 'must be formatted as json'}" + END_SEQ[0]).encode(ENCODING))
                return True, {'error': 'must be formatted as json'}
            except Exception as e:
                self.log.error("Failed to send error message back to client: %s", e)
                return False, {'error': 'failed to send message back to client'}
        # say we got the message and do the initial check to make
        # sure its valid
        return_message = self._parse_command(jdata)
        if return_message:
            self.log.debug("return message: %s", json.dumps(return_message))
            if 'client' in jdata and isinstance(self.engine, Game):
                # make sure the socket is in the socket:id map
                self.socket_id_map[connection] = jdata['client']
        if not return_message:
            self.log.critical("failed to parse %s and returned %s", jdata, return_message)
        try:
            connection.sendall((json.dumps(return_message) + END_SEQ[0]).encode(ENCODING))
            return True, jdata
        except Exception as e:
            self.log.error("failed to send response")
            self.log.error(e)
            return False, {}
        
    def _addstr_wrapper(self, string: str, width: int = None, height: int = None):
        """
        Trying to make our code more readable...

        Notes about curses library:
        stdsrc is the terminal being printed to
        the terminal window only gets updated when stdsrc.refresh() is called
        stdsrc.addstr throws an exception when you try to print out of the terminal window, so we *should*
        make sure we are catching that potential error so the server does not crash
        """
        if self.stdsrc:
            try:
                if isinstance(width, int) and isinstance(height, int):
                    self.stdsrc.addstr(width, height, string)
                else:
                    self.stdsrc.addstr(string)
            except:
                pass
    
    def run_server(self, stdsrc = None) -> None:
        """
        Single thread that handles multiple clients

        This function is designed to be run in a curses wrapper to avoid all of the 
        problems (https://docs.python.org/3/library/curses.html#curses.wrapper)
        with curses windows

        You can also initilize the window when creating the game server

        [PROG] clean up the print statements to make it look nice
        [PROG] clean this code
        [TODO] move screen clearing/refresh to self._addstr_wrapper()
        [TODO] move line number trackig to self._addstr_wrapper()
        [DONE] if we are a room server, shutdown when there is no one in the room
        [TODO] make the gui different if we are running a Cluster engine
        [TODO] track connected ip:port combos
        [TODO] 9x9 gui for individual rooms
                will have to deal with making curses threadsafe, but it should be fine by adding a wrapper
                https://stackoverflow.com/questions/51315269/multithreading-curses-output-in-python
        """
        if stdsrc:
            if self.stdsrc:
                self.log.warning("self.stdsrc was already defined but run_server is attempting to overwrite it")
            self.stdsrc = stdsrc
        if self.stdsrc:
            self.stdsrc.clear()
            self._addstr_wrapper("starting server...")
            self.stdsrc.refresh()
        last_broadcast = 0
        prev_num_lines = 0
        next_line = 0
        num_connections = 1
        self.log.info("starting poller")
        poller = select.poll()
        self.log.info("adding socket")
        poller.register(self.sock, READ_ONLY)

        fd_to_socket = { 
            self.sock.fileno(): self.sock,
        }
        need_to_clear = True
        can_shutdown = False
        shutdown_timeout = 5
        shutdown_requested = 0
        while True:
            # check if we need to broadcast
            need_to_broadcast = False

            # broadcast if we need to
            if self.nameserver_broadcast_interval > 0:
                if time() - last_broadcast > self.nameserver_broadcast_interval:
                    self._broadcast(self.nameserver_message)
                    last_broadcast = time()
            
            # check if it is time to shutdown
            if can_shutdown:
                if time() - shutdown_requested > shutdown_timeout:
                    # TODO: maybe timeout
                    # send the shutdown message
                    self._send_shutdown_message()
                    # maybe give up the log file and socket
                    self.sock.close()
                    self.log_file.close()
                    # return so the concurrent futures executor knows we are done
                    # right now nothing is happening with this message though
                    # all that matters is that the function stops running
                    return {'status': 'shutdown'}

            # TODO: change this
            if self.stdsrc:
                self.stdsrc.refresh()
                if need_to_clear:
                    self.stdsrc.clear()
                    need_to_clear = False
                self._addstr_wrapper(f"connected sockets: {num_connections}", 0, 0)
                for i, stat in enumerate(self.lifetime_stats):
                    self._addstr_wrapper(f"{stat}: {self.lifetime_stats[stat]}", i + 1, 0)
                next_line = len(self.lifetime_stats) + 2
                
            # grab the next open connection
            try:
                read_sockets = poller.poll(1000)
            except Exception as e:
                self.log.error("caught error when selecting socket: %s", e)
                read_sockets = []
            
            if not read_sockets:
                self._addstr_wrapper("...no sockets are ready", next_line, 0)
                next_line += 1
                can_shutdown = True
                shutdown_requested = time()
                continue
            
            self._addstr_wrapper(f"ready sockets: {len(read_sockets)}\n", next_line, 0)
            next_line += 1
            sockets_processed = 0
            for fd, event in read_sockets:
                sock = fd_to_socket[fd]
                if not event & READ_ONLY:
                    self.log.debug("skipping event because its not a input")
                    continue
                self.log.debug("type of sock: %s", type(sock))
                if sock is self.sock:
                    self.log.info("waiting for new client")
                    sock.listen(1)
                    new_sock, addr = sock.accept()
                    poller.register(new_sock, READ_ONLY)
                    fd_to_socket[new_sock.fileno()] = new_sock
                    num_connections += 1
                    if can_shutdown:
                            self.log.info("Room %s cleared the shutdown timer", self.id)
                    can_shutdown = False
                else:
                    need_to_broadcast = True
                    running, request = self._listen_to_client(sock)
                    if not running:
                        self.log.info("removing %s from sockets", sock)
                        # remove it from the connections list
                        # get the id of the socket
                        # if we are a Game Engine
                        if isinstance(self.engine, Game):
                            self.log.info("Client %s left room %s", self.socket_id_map[sock], self.id)
                            self.connections.pop(self.socket_id_map[sock])
                            # remove sock from sockets
                            self.socket_id_map.pop(sock)
                        poller.unregister(sock)
                        sock.close()
                        fd_to_socket.pop(fd)
                        num_connections -= 1

                        # check here if that was our last connection
                        # because if it was, we need to send 'shutdown_room'
                        # to the cluster server then shut ourself down
                        # NOTE: we will always have at least one connection (ourself)
                        # NOTE: due to performance reasons, we should wait a little bit
                        #       before actually shutting down so here we will just tag
                        #       the server as eligible to shutdown
                        if num_connections == 1 and isinstance(self.engine, Game):
                            can_shutdown = True
                            shutdown_requested = time()
                            self.log.info("Room %s started the shutdown timer", self.id)
                    else:
                        if can_shutdown:
                            self.log.info("Room %s cleared the shutdown timer", self.id)
                        can_shutdown = False
                        sockets_processed += 1
                        next_line += 1
            self._addstr_wrapper(f"number of sockets processed: {sockets_processed}")
            if prev_num_lines != next_line:
                prev_num_lines = next_line
                need_to_clear = True
            
            # I know, it is violating the whole polymorphism thing
            if isinstance(self.engine, Game):
                self.log.info("Clearing interactive markers")
                self.engine.clear_empty_markers()
                # if we interacted with clients that round we will need to broadcast the new room state
                if self.broadcast_with_udp:
                    if need_to_broadcast:
                        self._broadcast_current_room_state()
                        need_to_broadcast = False
            
            

def main():
    """
    The server for one room
    When run alone, it will broadcast itself,
    however, when we run a server cluster (many rooms)
    we would only want to broadcast the entry point

    Of course, since this server should only be interacted with via the ServerCluster,
    these args are only useful for our own testing
    """

    parser = argparse.ArgumentParser(prog='GameServer')
    parser.add_argument('project_name', type=str)
    parser.add_argument('--quiet', '-q', action='store_true')
    # NOTE: logging.getLevelNamesMapping is only available in 3.11 and up
    parser.add_argument('--logging_level', '-l', default=logging.DEBUG, type=int, choices=logging.getLevelNamesMapping().values())
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--gui', action='store_true')
    parser.add_argument('--log_file', type=str, default='')
    
    args = parser.parse_args()

    logging.basicConfig(level=args.logging_level)
    if args.quiet:
        logging.disable(logging.CRITICAL)
    port = args.port
    project_name = args.project_name
    
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setLevel(args.logging_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)

    server = GameServer("", port, project_name=project_name)
    if args.gui:
        import curses
        curses.wrapper(server.run_server)
    else:
        # run without the curses window
        server.run_server()

if __name__ == "__main__":
    main()
