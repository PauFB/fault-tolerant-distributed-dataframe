import logging
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# Set up logging
self_url = 'http://localhost:8000'
client_url = 'http://localhost:10000'
server = SimpleXMLRPCServer(('localhost', 8000), logRequests=True, allow_none=True)
logging.basicConfig(level=logging.INFO)

workers_list = list()
master_url = 'http://localhost:8000'


# Functions
def add_node(worker):
    workers_list.append(worker)


def remove_node(worker):
    workers_list.remove(worker)


def get_workers():
    return workers_list


def check():
    return True


def get_master():
    return master_url


def workers_fault_tolerance(url):
    w = ServerProxy(url, allow_none=True)
    try:
        w.check()
        print(url + " up")
    except ConnectionError:
        print(url + " down")
        workers_list.remove(url)


def serve4ever():
    server.serve_forever()


server.register_function(add_node)
server.register_function(remove_node)
server.register_function(get_workers)
server.register_function(check)
server.register_function(get_master)

# Start the server
try:
    print('Use Ctrl+c to exit')
    x = threading.Thread(target=serve4ever, daemon=True)
    x.start()
    while True:
        for node in workers_list:
            workers_fault_tolerance(node)
except KeyboardInterrupt:
    print('Exiting')
