import logging
from xmlrpc.server import SimpleXMLRPCServer

# Set up logging
server = SimpleXMLRPCServer(('localhost', 8000), logRequests=True, allow_none=True)
logging.basicConfig(level=logging.INFO)

workers_list = list()


# Functions
def add_node(node):
    workers_list.append(node)


def remove_node(node):
    workers_list.remove(node)


def get_workers():
    return workers_list


server.register_function(add_node)
server.register_function(remove_node)
server.register_function(get_workers)

# Start the server
try:
    print('Use Ctrl+c to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print('Exiting')
