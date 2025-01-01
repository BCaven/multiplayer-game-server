"""
Server Cluster

This server cluster will broadcast itself to the name server and send new clients to individual
room servers

This server should have a log/ckpt which has the rooms setup - each room has its own log/ckpt
for the state of the individual room

Comprised of a threadpool that has:
    One thread handling the "cluster" operations
    N threads each handling a room
"""
from typing import Dict, Callable
import argparse
import logging
from GameEngine import Engine
from concurrent.futures import ThreadPoolExecutor
from GameServer import GameServer

class Cluster(Engine):
    """
    Cluster Engine for running the central cluster thread

    The idea is that new clients first call
    'register_new_client' if they do not have an id
    then
    'get_room_server' when they want to go to a new room

    room servers call
    'shutdown_room' when there is no one in them
    TODO: make sure we do not end up with zombie threads

    logs and checkpoints:
        I do not think the cluster needs to log anything right now
        since rooms store where clients were last and the ips are
        constantly changing so there is no need to remember it
        In theory we could want to store self.lifetime_clients
        but for now I think it is fine to not store anything
    
    TODO: check for other clusters
    """
    def __init__(self, **kargs):
        self.log = logging.getLogger(__name__)
        self.args = kargs
        self.log.critical("CLUSTER HAS STARTED")
        self.addr = f"{kargs['host']}:{kargs['port']}"
        self.host = kargs['host']
        self.lifetime_clients = 0
        # last location of each client
        self._clients: dict = {}
        # I need to decide if this is going to store the
        # futures objects or if it is going to store the 
        # gameserver object
        # I think for now it might be fine to not store the futures object
        # also
        # TODO: make sure GameServer is threadsafe
        self._server_map: Dict[int, GameServer] = {
            # id: GameServer
        }
        self.command_map: Dict[str, Callable[[int], dict]] = {
            'register_new_client': self.register_new_client,
            'get_room_server': self.get_room_server,
            'shutdown_room': self.shutdown_room,
        }
        # TODO: check if we should give a max_workers arg to the executor
        # or if it doesn't matter for right now
        self.executor = ThreadPoolExecutor()
        self.futures: dict = {}
    
    def _generate_client_id(self):
        """Return a unique client id"""
        self.lifetime_clients += 1
        return self.lifetime_clients
    
    def get_room_server(self, id):
        """
        Get the address of a specific room

        we are passing the id of the server we want to go to

        TODO: make sure self.host is real and not localhost
        """
        if id in self._server_map:
            return {'addr': f"{self.host}:{self._server_map[id].port}"}
        # if we did not find the server, we need to start a new one
        # TODO: fun GUI things iff everything else gets finished first
        udp = False
        if 'use_udp' in self.args:
            udp = True
        self._server_map[id] = GameServer("", 
                                          id=id,
                                          port=0, 
                                          log=f"game{id}.log", 
                                          checkpoint=f"game{id}.ckpt", 
                                          # set the info log file
                                          info_log_file=f"game{id}.info",
                                          nameserver_broadcast_time=-1,
                                          stdsrc=None,
                                          nameserver=self.addr,
                                          broadcast_with_udp=udp)
        # NOTE: this is potentially dangerous...
        #       since a room *could* never shut down
        #       in practice, this is fine
        self.log.info("Submitting room %s to run in a background thread", id)
        self.futures[id] = self.executor.submit(self._server_map[id].run_server, None)
        return {'addr': f"{self.host}:{self._server_map[id].port}"}
    
    def shutdown_room(self, client):
        """
        A room has been shutdown, check the status of room threads and remove dead ones from the server_map
        
        client is the one that is dead
        """
        self.log.critical("Starting removal process for room %s", client)
        # remove the room from the map
        # first confirm that the thread is actually dead
        if not self.futures[client].done():
            self.log.warning("The room was shut down but it is not actually finished shutting down")
        
        # wait for the server to shut down
        result = self.futures[client].result()
        self.log.info("Recieved %s from server %s as it shutdown", result, client)
        
        # now we should be ok to delete the data
        try:
            self.log.info("deleting room %s", client)
            del self._server_map[client]
        except Exception as e:
            self.log.warning("Caught error when trying to delete room %s: %s", client, e)
        
        return {'success': f'room {client} has been removed'}
    
    def register_new_client(self, client):
        """
        Register new client in the list of known clients
        """
        # step one: get the client info
        if client in self._clients:
            # the client already exists
            return {'client_id': client, 'last_room': self._clients[client]}
        # TODO: decide how we want to get/send/store the client info and what we even need in the first place
        # step two: add the client to the client map
        # just put it in pos zero
        self._clients[client] = 0
        # step three: uhhhhh
        return {'client_id': client, 'last_room': 0}



def main():
    """
    The server for managing the entire cluster


    """
    parser = argparse.ArgumentParser(prog='GameCluster')
    parser.add_argument('project_name', type=str)
    parser.add_argument('--verbose', '-v', action='store_true')
    # NOTE: logging.getLevelNamesMapping is only available in 3.11 and up
    parser.add_argument('--logging_level', '-l', default=logging.DEBUG, type=int, choices=logging.getLevelNamesMapping().values())
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--gui', action='store_true')
    parser.add_argument('--log_file', type=str, default='')
    parser.add_argument('--use_udp', action='store_true')
    
    args = parser.parse_args()

    logging.basicConfig(level=args.logging_level)
    if not args.verbose:
        logging.disable(logging.CRITICAL)
    
    port = args.port
    project_name = args.project_name
    
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setLevel(args.logging_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
    
    server = GameServer("", port, 
                        project_name=project_name,
                        engine_type=Cluster,
                        log='cluster.log',
                        checkpoint='cluster.ckpt',
                        broadcast_with_udp=args.use_udp)
    if args.gui:
        import curses
        curses.wrapper(server.run_server)
    else:
        # run without the curses window
        server.run_server()


if __name__ == "__main__":
    """
    The server cluster should be the only thing getting manually launched
    
    And we need to start it here
    """
    main()