"""
Stress test server cluster by generating a lot of clients and having them slam the server with
requests
"""
from GameClient import GameClient
from random import randint, choice
from concurrent.futures import ThreadPoolExecutor
import argparse
import time
import pickle

NUM_CLIENTS = 1
NUM_MOVES = 100
MOVES = ["up", "down", "left", "right", "interact", "new_room", "get_room"]
operations_by_client = {}

def execute_random_commands(client: GameClient):
    '''
    Randomly select a move from the list of moves, get the method call associated with that
    move, and have the client call it. Once NUM_MOVES moves are executed, close the client
    '''
    operations = {move: {"time": 0, "count": 0, "max": 0} for move in MOVES}
    for _ in range(NUM_MOVES):
        move = choice(MOVES)
        start = time.time()
        move_function = getattr(client, move)
        if move != "new_room":
            move_function()
        else:
            # new room wants to go to a new place, we will randomly go one left or one right
            move_function(client.current_room_number + randint(-1, 1))
        end = time.time()
        elapsed = end - start
        operations[move]["time"] += elapsed
        operations[move]["count"] += 1
        if elapsed > operations[move]['max']:
            operations[move]['max'] = elapsed
    operations_by_client[client] = operations
    client._close()
    
def move_times():
    '''
    Get the overall time for each operation as well as the number of times it was executed.
    Stored this data by client to avoid race conditions from concurrent events adjusting counts
    '''
    ops = {move: {"time": 0, "count": 0, "max": 0} for move in MOVES}
    for _, data in operations_by_client.items():
        for move, stats in data.items():
            ops[move]["time"] += stats["time"]
            ops[move]["count"] += stats["count"]
            if stats["max"] > ops[move]["max"]:
                ops[move]["max"] = stats["max"]
    return ops

def display_move_stats(times):
    '''
    Display the throughput and average latency for each move
    '''
    print(f"**Basic Move Statistics with {NUM_CLIENTS} Clients**")
    print('-' * 40)
    for move, stats in times.items():
        throughput = stats["count"] / stats["time"]
        latency = 1/throughput
        print(f"{'Move:':<10} {move}")
        print(f"{'Throughput:':<15} {throughput:,.2f} ops/sec")
        print(f"{'Latency:':<15} {latency:,.4f} sec")
        print('-' * 40)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Test Server Cluster Performance")
    parser.add_argument('project_name', type=str)
    parser.add_argument('--quiet', '-q', action='store_true')
    parser.add_argument('--max_clients', '-max', type=int, default=100)
    parser.add_argument('--starting_clients', '-c', type=int, default=1)
    parser.add_argument('--num_moves', type=int, default=100)

    
    args = parser.parse_args()
    if args.quiet:
        import logging
        logging.disable(logging.CRITICAL)

    NUM_MOVES = args.num_moves
    NUM_CLIENTS = args.starting_clients
    total_data: dict = {}
    while NUM_CLIENTS <= args.max_clients:
        # Generate clients
        clients = [GameClient(args.project_name, i) for i in range(NUM_CLIENTS)]
        # Concurrently have each client execute commands
        with ThreadPoolExecutor(max_workers=NUM_CLIENTS) as executor:
            executor.map(execute_random_commands, clients)   
        ops = move_times()
        display_move_stats(ops)
        total_data[NUM_CLIENTS] = ops
        # clear to make sure we avoid exhausing the number of ports we can have
        del clients
        NUM_CLIENTS *= 2
    
    # pickle it all
    # so the data can be processed
    with open(f"perf_test_{args.max_clients}.pkl", 'wb') as f:
        pickle.dump(total_data, f)
    
