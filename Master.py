import logging
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# Set up logging
server = SimpleXMLRPCServer(('localhost', 8000), logRequests=True, allow_none=True)
logging.basicConfig(level=logging.INFO)

# URLs
self_url = 'http://localhost:8000'
client_url = 'http://localhost:10000'

workers_list = list()


# Server functions
def add_node(worker_url):
    workers_list.append(worker_url)


def remove_node(worker_url):
    workers_list.remove(worker_url)


def get_workers():
    return workers_list


def check_worker_availabilities():
    for url in workers_list:
        w = ServerProxy(url, allow_none=True)
        try:
            w.check()
            print(url + " up")
        except ConnectionError:
            print(url + " down")
            # Remove any unreachable node that causes a connection error
            workers_list.remove(url)


def serve_forever():
    server.serve_forever()


server.register_function(add_node)
server.register_function(remove_node)
server.register_function(get_workers)

# Start the server
try:
    print('Use Ctrl+c to exit')
    # Start the master server in parallel
    x = threading.Thread(target=serve_forever, daemon=True)
    x.start()
    while True:
        check_worker_availabilities()
except KeyboardInterrupt:
    print('Exiting')
