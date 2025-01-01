"""
Game Client

RPC format:
{
"method": str,
"client": int
}

TODO: make encoding a constant instead of being hard coded
"""
import argparse
import socket
import logging
import json
from time import sleep, time
import select
import curses
from threading import Lock
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor

END_SEQ = ("END_OF_MESSAGE", "ALT_TERMINATION")
ROOM_SIZE = 9
CLIENT_CHAR = '@'
INTERACTABLE_CHAR = '?'
READ_ONLY = ( select.POLLIN |
              select.POLLPRI |
              select.POLLHUP |
              select.POLLERR )

class GameClient:
    """
    GameClient
    """
    def __init__(self,
                 project: str,
                 client_id: str,
                 nameserver: str = 'catalog.cse.nd.edu:9097',
                 owner: str = "me",
                 stdsrc = None,
                 max_retries: int = -1,
                 max_resends: int = -1,
                 retry_time: int = 1,
                 recv_timeout: int = 10):
        """
        Connect to a game server

        Ask the nameserver for an available server

        NOTE: retry_time is a modifier for how long the client should wait when retrying
        """
        # solve all our problems by saying "nah, forget about it"
        socket.setdefaulttimeout(recv_timeout)

        self.log = logging.getLogger(__name__)
        self.stdscr = stdsrc
        # keep track of our ID
        self.id = client_id

        # connect to server cluster and starting room
        self.cluster_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.current_room_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # calculating the udp ip and port beforehand to avoid doing multiple calculations
        # This is only useful if you can do UDP in the first place
        hname = socket.gethostname()
        self.host = socket.gethostbyname(hname)
        # also going to need a socket for listening to server broadcasts
        self.broadcast_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.broadcast_listener.bind((self.host, 0)) # just grab the first available port
        except TimeoutError as e:
            self.log.warning("Timed out when trying to bind broadcast listener")
        self.project = project
        self.nameserver = nameserver
        self.owner = owner
        self.CHECK_OWNER = True if owner else False
        self.MAX_RETRIES = max_retries
        self.MAX_RESENDS = max_resends
        self.RETRY_MODIFIER = retry_time
        self.RECV_TIMEOUT = recv_timeout
        self._connect_to_server()
        # Board for displaying
        self.board = None
        self.board_lock = Lock()
        self.running_frame = 0

        #Connecting the client to the last known room number, or defaulting to zero if there isn't a last known room
        try:
            self.current_room_number = self.register_new_client()['last_room']
        except Exception as e:
            self.log.warning("encounter error %s while finding last known room number, defaulting to room 0")
            self.current_room_number = 0
        self.new_room(self.current_room_number)
        self.log.info("Successfully initialized GameClient")

    def _connect_to_server(self, attempts: int = 0):
        """
        A helper function for all those times we tell the server we love it and it doesnt say anything back

        obv we just need to say it again.
        """
        if attempts > self.MAX_RETRIES and self.MAX_RETRIES > 0:
            self.log.critical("failed to connect to server - max timeout exceeded")
            # we failed to connect
            # we should probably throw an error
            raise Exception("Failed to connect to server - max retries exceeded")
        try:
            addr, port = self._find_server()
            self.cluster_socket.connect((addr, port))
            self.log.info("Connected to %s : %s", addr, port)
        except Exception as e:
            self.log.critical("Failed to connect to server %s : %s", addr, port)
            self.log.critical("caught exception: %s", e)
            self.cluster_socket.close()
            # make a new socket
            self.cluster_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # obv we retry, but we should probably sleep before we retry
            # we are going to wait longer every time we fail
            sleep((attempts + 1) * self.RETRY_MODIFIER)
            self._connect_to_server(attempts=attempts + 1)
    
    def _find_server(self):
        """
        Find a server

        We will want to grab the fastest server

        It is assumed that name servers will have a /query.json page that is a list of dictionaries.
        Each entry has at least the following information:
        {
            'type': str,
            'project': str,
            'address': ip4 addr,
            'port': int,
            'lastheardfrom': int
        }
        """
        try:
            # NOTE: self.nameserver = addr:port
            ns_addr = f"http://{self.nameserver}/query.json"
            response = urlopen(ns_addr).read()
            json_response = json.loads(response)
            # find a server that matches our type and project name
            # should also check owner so we do not grab someone else's game server
            chosen_server = (None, None)
            last_response = 0
            for server in json_response:
                if 'type' in server:
                    if server['type'] != 'game_server':
                        continue
                else:
                    continue
                if 'project' in server:
                    if server['project'] != self.project:
                        continue
                else:
                    continue
                if self.CHECK_OWNER:
                    if 'owner' in server:
                        if server['owner'] != self.owner:
                            continue
                    else:
                        continue
                if 'address' not in server:
                    continue
                if 'port' not in server:
                    continue
                if 'lastheardfrom' not in server:
                    continue
                if server['lastheardfrom'] < last_response:
                    # we want the latest (largest) time possible
                    continue
                # if all of those passed then we found a good server
                chosen_server = (server['address'], server['port'])
                last_response = server['lastheardfrom']
            # we couldnt find a good server
            if chosen_server == (None, None):
                self.log.error("no server found in %s", self.nameserver)
            return chosen_server
        except Exception as e:
            self.log.error("failed to connect to nameserver: %s", e)
            return None, None

    def _recv_all(self, sock: socket.socket, n):
        """
        Call recv until you have gotten all you are going to get from one message

        Recv until you get a string that ends with the END character

        If we havent recieved any data in awhile, but we were supposed to,
        fail and raise an error (that gets caught by the function that called us)
        """
        data = ""
        start_time = time()
        poller = select.poll()
        poller.register(sock, READ_ONLY)
        while True:
            # NOTE: originally we used select here
            #       but switched to poll to avoid 'filedescriptor out of range'
            #       since poll does not have that limit
            try:
                self.log.info("Starting the poller to listen for responses")
                read_sockets = poller.poll(self.RECV_TIMEOUT * 1000)
            except Exception as e:
                self.log.error("caught error when selecting socket: %s", e)
                read_sockets = []
            
            if not read_sockets:
                self.log.warning("The poller timed out and did not see any available sockets")
                raise TimeoutError("Socket timed out when reading")
            
            # if we didnt timeout, we know the socket has things to read
            new_data = sock.recv(n).decode('utf-8')
            if not new_data:
                self.log.warning("There was no new data to be found")
                raise TimeoutError("Socket did not return anything")
            
            self.log.debug("Recieved new data: %s", new_data)
            if time() - start_time > 1000:
                self.log.warning("recv has been recieving for %s seconds", time() - start_time)
            for ending in END_SEQ:
                if new_data[-1 * len(ending):] == ending:
                    return data + new_data[:-1*len(ending)]
            data += new_data
            
    def _send_and_recv(self, message: dict, sock: socket, attempts: int = 0) -> dict:
        """
        Every method uses this, so we might as well only write it once

        Since we can retry this *AND* _find_server we technically could wait forever (until the server is found), try the command again, repeat
        However, since they use two different max values, you might want to try the server five times, but only attempt each message once
        """
        self.log.info(message)
        if self.MAX_RESENDS > 0:
            # if we set MAX_RETRIES to <0, we never timeout
            if attempts > self.MAX_RESENDS:
                return {'error': 'max retries exceeded'}
        if 'row' in message:
            if message['row'] < 0:
                return {'error': f"invalid row {message['row']}"}
        if 'col' in message:
            if message['col'] < 0:
                return {'error': f"invalid col {message['col']}"}
        try:
            self.log.debug("Trying %s", message)
            sock.sendall((json.dumps(message) + END_SEQ[0]).encode('utf-8'))
            data = self._recv_all(sock, 1024)
            jdata = json.loads(data)
            if 'error' in jdata:
                self.log.error("error: %s", jdata['error'])
            return jdata
        except Exception as e:
            # say we failed
            self.log.warning("Failed %s", message)
            self.log.error(e)
            if sock == self.cluster_socket:
                self.cluster_socket.close()
                self._connect_to_server()
                # retry the command now that we have a server
                self.log.info("Retrying %s", message)
                self._send_and_recv(message, sock=self.cluster_socket, attempts=attempts + 1)
            elif sock == self.current_room_socket:
                return {'error': f'failed to send {message}'}

    def _close(self):
        """Close the sockets"""
        self.cluster_socket.close()
        self.current_room_socket.close()

    def new_room(self, room_number) -> dict:
        """
        Asks the server cluster for the port of a given room number
        """
        message = {
            'method': 'get_room_server',
            'client': room_number
        }
        
        response = self._send_and_recv(message, sock=self.cluster_socket)
        if not response:
            response = {
                "result": {'error': 'no response from server'},
                "addr": ":"
                }

        self.current_room_number = room_number

        
        addr, port = response['addr'].split(':')
        self.current_room_socket.close()
        # make a new socket
        self.current_room_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Connects the to the room socket and then tells the room that we have connected to it
            self.current_room_socket.connect((addr, int(port)))
            self.add_to_room()
        except Exception as e:
            # We sleep here so we don't overload the server with requests
            self.log.warning("Caught exception when trying to connect to room %s: %s", self.current_room_number, e)
            sleep(1)
            self.new_room(self.current_room_number)
        return response
    
    def register_new_client(self) -> dict:
        """
        Registers the client with the cluster, allowing the cluster to track all clients
        """
        message = {
            'method': 'register_new_client',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.cluster_socket)
        if not response:
            response = {"result": {'error': 'no response from server'}}
        return response

    def add_to_room(self) -> dict:
        """
        Adds the client to the current room number
        """
        udp_port = self.broadcast_listener.getsockname()[1]
        self.log.info("UDP INFO: %s, %s", self.host, udp_port)
        message = {
            'method': 'add_client',
            'client': self.id,
            'broadcast_addr': f"{self.host}:{udp_port}"
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {"result": {'error': 'no response from server'}}
        return response

    def get_room(self) -> dict:
        """
        Get the room, duh
        """
        message = {
            'method': 'get_room',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {"result": {'error': 'no response from server'}}
        if 'room' in response:
            self.board = response['room']
            return response
        else:
            self.log.warning("asked for the room state but did not get a room")
            return {'error': 'no response from server'}

    def up(self) -> dict:
        """
        Go up
        """
        message = {
            'method': 'up',
            'client': self.id
        }
        
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {'error': 'no response from server'}
        return response

    def down(self) -> dict:
        """
        Go down
        """
        message = {
            'method': 'down',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {'error': 'no response from server'}
        return response
    
    def left(self) -> dict:
        """
        Go left
        """
        message = {
            'method': 'left',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {'error': 'no response from server'}
        if 'success' in response:
            if response['success'] == 'exit left':
                self.log.info("Going to the next room: %s", self.current_room_number - 1)
                self.new_room(self.current_room_number - 1)
        return response
    
    def right(self) -> dict:
        """
        Go right
        """
        message = {
            'method': 'right',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {'error': 'no response from server'}
        if 'success' in response:
            if response['success'] == 'exit right':
                self.log.info("Going to the next room: %s", self.current_room_number + 1)
                self.new_room(self.current_room_number + 1)
        return response

    def interact(self) -> dict:
        """
        Interact with an object you are standing on
        """
        message = {
            'method': 'interact',
            'client': self.id
        }
        response = self._send_and_recv(message, sock=self.current_room_socket)
        if not response:
            response = {'error': 'no response from server'}
        return response
    
    def _addstr_wrapper(self, string: str, width: int = None, height: int = None, color: int = 0):
        """
        Trying to make our code more readable...

        Notes about curses library:
        stdsrc is the terminal being printed to
        the terminal window only gets updated when stdsrc.refresh() is called
        stdsrc.addstr throws an exception when you try to print out of the terminal window, so we *should*
        make sure we are catching that potential error so the server does not crash
        """
        if self.stdscr:
            try:
                if isinstance(width, int) and isinstance(height, int):
                    self.stdscr.addstr(width, height, string)
                else:
                    self.stdscr.addstr(string)
            except:
                pass
    
    def game_GUI(self, stdscr, POLL_ROOM: bool = True, MAX_FAILED_ATTEMPTS: int = 5, ROOM_REFRESH_WAIT: int = 50):
        """
        Thread function to display the game's GUI

        TODO: More pretty stuff to display to the screen
        """
        interact_text = ""
        self.stdscr = stdscr
        stdscr.nodelay(True) # makes the keywait non-blocking
        curses.start_color() # Enabling changing colors for distinct player client
        curses.noecho()
        curses.curs_set(0)
        stdscr.keypad(True)
        stdscr.timeout(50)

        curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
        
        last_direction = "none"
        saved_board = {}
        
        failed_attempts = 0
        while True:
            if failed_attempts > MAX_FAILED_ATTEMPTS:
                # we failed five consecutive times
                self.log.warning("Failed %s times so we are reconnecting to the central server", failed_attempts)
                self._connect_to_server()
                try:
                    self.register_new_client()
                except Exception as e:
                    self.current_room_number = 0
                    self.log.warning("encounter error %s while finding last known room number, defaulting to room %s", e, self.current_room_number)

                self.new_room(self.current_room_number)
                self.log.info("Successfully restarted connection")
                pass

            if POLL_ROOM:
                if int(time() * 100) % ROOM_REFRESH_WAIT == 0:
                    r = self.get_room()
                    if 'error' in r:
                        failed_attempts += 1
                    else:
                        failed_attempts = 0
            stdscr.clear()
            if self.board:
                saved_board = self.board
            
            #stdsrc.refresh()
            self._addstr_wrapper(f"Last command:          ", 0, 0)
            self._addstr_wrapper(f"Last command: {last_direction}", 0,0)
            self._addstr_wrapper(f"Current Room:           ", 1, 0)
            self._addstr_wrapper(f"Current Room: {self.current_room_number}", 1, 0)
            self._addstr_wrapper("Controls:\nleft: [a] or [left arrow]\nright: [d] or [right arrow] \nup: [w] or [up arrow] \ndown: [s] or [down arrow]\ninteract: [e] or [space]\nquit: [q]", 3,0)
            if failed_attempts > 0:
                self._addstr_wrapper(f"Failed to connect to the server {failed_attempts} times", 12, 0)
                self._addstr_wrapper("Retrying connection...", 13, 0)
            self._addstr_wrapper(f"INTERACT: {interact_text}", 16, 0)
            self._addstr_wrapper(f"BOARD: {saved_board}", 15, 0)
            
            # TODO: make this not hard coded
            for x in range(0, 11):
                for y in range(0, 11):
                    self._addstr_wrapper('.', 12-y, 38+x*2)
                    if x % 10 == 0 or y % 10 == 0:
                        self._addstr_wrapper('#', 12-y, 38+x*2)
                
            for thing, position in saved_board.items():
                x_pos, y_pos = position.split(':')
                x_pos = int(x_pos)
                y_pos = int(y_pos)
                try:
                    if int(thing) == int(self.id):
                        #self.addstr(11 - y_pos, 40 + x_pos * 2, CLIENT_CHAR, curses.color_pair(1))
                        self._addstr_wrapper(CLIENT_CHAR, 11 - y_pos, 40 + x_pos * 2)
                    else:
                        self._addstr_wrapper('&', 11 - y_pos, 40+ x_pos*2)
                    
                except:
                    self._addstr_wrapper(INTERACTABLE_CHAR, 11 - y_pos, 40+ x_pos*2)


            self._addstr_wrapper("", 18, 0)

            # Finding inputs from the user
            key = stdscr.getch()
            if key == ord('q') or key == curses.KEY_EOS:
                break
            elif key == curses.KEY_UP or key == ord('w'):
                r = self.up()
                if 'error' in r:
                    failed_attempts += 1
                else:
                    failed_attempts = 0
                last_direction = "up"
            elif key == curses.KEY_DOWN or key == ord('s'):
                r = self.down()
                if 'error' in r:
                    failed_attempts += 1
                else:
                    failed_attempts = 0
                last_direction = "down"
            elif key == curses.KEY_LEFT or key == ord('a'):
                r = self.left()
                if 'error' in r:
                    failed_attempts += 1
                else:
                    failed_attempts = 0
                last_direction = "left"
            elif key == curses.KEY_RIGHT or key == ord('d'):
                r = self.right()
                if 'error' in r:
                    failed_attempts += 1
                else:
                    failed_attempts = 0
                last_direction = "right"
            elif key == ord(' ') or key == ord('e'):
                last_direction = "interact"
                stdscr.move(16, 0)
                stdscr.clrtoeol()
                stdscr.refresh()
                r = self.interact()
                if 'msg' in r:
                    interact_text = r['msg']
                
                if 'error' in r:
                    failed_attempts += 1
                else:
                    failed_attempts = 0

            else:
                pass
        self._close()
    
    def _game_command_listener(self):
        """
        Thread function listens for commands and sends them to the current room server
        """

        while True:
            self.log.info("listening for udp broadcast")
            response = json.loads(self._recv_all(self.broadcast_listener, 1024))
            self.log.debug("UDP listener recieved: %s", response)
            if response['frame'] > self.running_frame:
                self.running_frame = response['frame']
                if response['room_id'] is self.current_room_number:
                    self.log.debug("The incoming room: %s", response['room'])
                    # Now that we have verified that the frame is valid we can actually move everyting to the display
                    with self.board_lock:
                        # We are assuming that all player IDs are numbers
                        self.board = response['room']

            

def main():
    """
    Main Game Client
    """
    parser = argparse.ArgumentParser(prog='GameClient')
    parser.add_argument('project_name', type=str)
    parser.add_argument('client_id', type=str)
    parser.add_argument('--verbose', '-v', action='store_true')
    # NOTE: logging.getLevelNamesMapping is only available in 3.11 and up
    parser.add_argument('--logging_level', '-l', default=logging.DEBUG, type=int, choices=logging.getLevelNamesMapping().values())
    parser.add_argument('--log_file', type=str, default='client.info')
    parser.add_argument('--udp_listener', action='store_true')
    parser.add_argument('--max_retry_attempts', type=int, default=5)
    parser.add_argument('--refresh_wait', type=int, default=50)

    args = parser.parse_args()

    logging.basicConfig(level=args.logging_level)
    if not args.verbose:
        logging.disable(logging.CRITICAL)
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setLevel(args.logging_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)

    project_name = args.project_name
    client_id = args.client_id

    
    client = GameClient(project=project_name, client_id=client_id)
    executor = ThreadPoolExecutor(max_workers=1)
    if args.udp_listener:
        executor.submit(client._game_command_listener)


    curses.wrapper(client.game_GUI, POLL_ROOM=not args.udp_listener, MAX_FAILED_ATTEMPTS=args.max_retry_attempts, ROOM_REFRESH_WAIT=args.refresh_wait)
    
    executor.shutdown(wait=False)

    

if __name__ == "__main__":
    main()
