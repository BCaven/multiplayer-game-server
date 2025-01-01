"""
GameEngine.py
"""

from abc import ABC
from typing import Dict, Callable
from random import choice, randint
import logging

# Interactive items need the following stats:
# name: str
# uses: int
# use_message: str
# empty_message: str
# conflict_message: str
# they will also have an 'emptied_this_round' flag
INTERACTIVE_ITEMS = [
    {
        'name': 'chest',
        'uses': 10,
        'use_message': 'you put your hand in the box and get a surprise',
        'empty_message': 'you put your hand in an empty box',
        'conflict_message': "you put your hand in the box and feel someone else's hand",
        'emptied_this_round': False
    },
    {
        'name': 'fire',
        'uses': 5,
        'use_message': 'ow thats hot',
        'empty_message': 'someone cooked here',
        'conflict_message': 'you approach the fire but it is too crowded and you cannot find a spot',
        'emptied_this_round': False
    }
]
INTERACT_FAIL_MESSAGES = [
    'you tried but there was nothing there',
    'you reach out and are disappointed',
    'you interact with the floor',
    'you tried to become one with the floor',
    'slow it down, not right now'
]
# Going to make these strings that format gets called on
# You have access to {collided_users} which is just a list of the users you collided with
INTERACT_ON_OTHER_USER = [
    'You look at {collided_users} awkwardly',
    '{collided_users} stare at you, you cant help but notice their concerned looks',
    '{collided_users} turn to look at you',
    '...hi!',
    'WHAT ARE YOU LOOKING AT?!?'
]

class Engine(ABC):
    """
    Since the cluster needs to be able to tell game engines what their nameserver is
    the engines need to be given their host and port............

    but since we want to make it so you can pass any engine they all need to have these arguments

    I might give up on polymorphism to be honest.
    """
    def __init__(self, host, port, **kargs):
        self.command_map: Dict[str, Callable[[int], dict]]
        self.clients: dict
        self.checkpoint_items
        self.kargs = kargs
        pass


class Game(Engine):
    """
    Game Engine for running one room
    """
    def __init__(self, **kargs):
        """
        Game for one room

        Nothing happens with the host and port, they are just here because
        Blake is making his life harder
        
        Game Board:
        List describing current board state
        When first creating a room we should randomly populate with interactive items

        Things Game Engines need to store in a checkpoint:
        self.clients
        self.room
        """
        self.log = logging.getLogger(__name__)
        # map clients to positions on the board
        self.clients: dict = {
            # client_id : position
        }
        # create the map of commands
        # not sure if this typehint is actually helping anyone
        self.command_map: Dict[str, Callable[[int], dict]] = {
            'add_client': self.add_client,
            'up': self.up,
            'down': self.down,
            'left': self.left,
            'right': self.right,
            'interact': self.interact,
            'get_room': self.get_room
        }
        self.room_dimension = 8

        x = randint(0,self.room_dimension)
        y = randint(0,self.room_dimension)
        self.room = {
           f"{x}:{y}" : choice(INTERACTIVE_ITEMS),
           '1:1': {
                    'name': 'chest',
                    'uses': 10,
                    'use_message': 'you put your hand in the box and get a surprise',
                    'empty_message': 'you put your hand in an empty box',
                    'conflict_message': "you put your hand in the box and feel someone else's hand",
                    'emptied_this_round': False
            }
        }
        self.log.info("Game started with the following room: %s", self.room)
    
    def add_client(self, client):
        """
        Add a client to the self.clients dict
        """
        if client in self.clients:
            return {'client_id': client, 'pos': self.clients[client]}
        # TODO: check with the cluster to get this client's data...
        # oh wait, no one cares
        self.clients[client] = f"{self.room_dimension//2}:{self.room_dimension//2}"
        return {'client_id': client, 'pos': self.clients[client]}
    
    def _move(self, client, x, y):
        """
        Move a client
        TODO: self.room, _move, and addclient, init
        putting this in a helper function so I don't have the same code four times
        """
        # get x and y from the single int position
        client_x, client_y = map(int, self.clients[client].split(':'))
        # make sure the desired location is still in the room
        # but we are going to do something goofy because I hate
        # readable code
        desired_x = client_x + x
        new_x = sorted((0, desired_x, self.room_dimension))[1]
        desired_y = client_y + y
        new_y = sorted((0, desired_y, self.room_dimension))[1]
        self.clients[client] = f"{new_x}:{new_y}"

        # now check if we left so we can go to a new room
        if new_x != desired_x:
            return True
        if new_y != desired_y:
            # We don't care as for the scope of this project, there are only rooms going left and right
            # If we wanted to implement rooms on all four axes we'd do it here though
            pass
        return False

    def up(self, client: dict):
        """
        Move a client up one tile
        """
        if client not in self.clients:
            return {'error': 'client not in room'}
        
        self._move(client, 0, 1) # up
        return {'success': 'move up'} # technically this isnt required, we just need something to say we didnt mess up
    
    def down(self, client):
        """
        Move a client down one tile
        """
        if client not in self.clients:
            return {'error': 'client not in room'}
        
        self._move(client, 0, -1) # down
        return {'success': 'move down'} # same as up

    def left(self, client):
        """
        Move a client left one tile
        """
        if client not in self.clients:
            return {'error': 'client not in room'}
        
        if self._move(client, -1, 0): # left
            return {'success': 'exit left'}
        return {'success': 'move left'}
    
    def right(self, client):
        """
        Move a client right one tile
        """
        if client not in self.clients:
            return {'error': 'client not in room'}
        
        if self._move(client, 1, 0): # right
            return {'success': 'exit right'}
        return {'success': 'move right'}

    def interact(self, client):
        """
        A client tries to interact with something

        name: str
        uses: int
        use_message: str
        empty_message: str
        conflict_message: str
        emptied_this_round: bool
        """
        # check if the client is on an interactive item
        pos = self.clients[client]
        msg = ""
        if pos in self.room:
            if not self.room[pos]['emptied_this_round']:
                if self.room[pos]['uses'] == 0:
                    msg = self.room[pos]['empty_message']
                else:
                    self.room[pos]['uses'] -= 1
                    msg = self.room[pos]['use_message']
                    # check if we just emptied it
                    if self.room[pos]['uses'] == 0:
                        self.room[pos]['emptied_this_round'] = True
            else:
                msg = self.room[pos]['conflict_message']
        else:
            # you tried to interact but there was nothing there
            msg = choice(INTERACT_FAIL_MESSAGES)
            # O(n) because of the way we set up the client dictionary
            # could make it O(1) if we stored clients in both self.room and self.clients
            # but its not worth it right now
            if matching_clients := [c for c in self.clients if c != client and self.clients[c] == pos]:
                # we are on top of someone else
                # doing the grammar checks
                if len(matching_clients) > 1:
                    matching_clients[-1] = 'and ' + matching_clients[-1]
                if len(matching_clients) > 2:
                    matching_clients_str = ', '.join(matching_clients)
                else:
                    matching_clients_str = ' '.join(matching_clients)
                msg = choice(INTERACT_ON_OTHER_USER).format(collided_users=matching_clients_str)
        return {'msg': msg}
    
    def clear_empty_markers(self):
        """
        Clear all the emptied_this_round flags
        """
        for _, interactable in self.room.items():
            interactable['emptied_this_round'] = False

    def get_room(self, connected_clients):
        """
        When requested, return the current room
        """
        room_items = {val['name']: key for key, val in self.room.items()}
        room_items |= connected_clients
        message = {
            # we need all of the things in the room
            'room': room_items,
        }
        return message
