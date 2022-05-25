import logging
import pickle
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import pandas as pd

# Set up logging
worker = SimpleXMLRPCServer(('localhost', 9000), logRequests=True, allow_none=True)
logging.basicConfig(level=logging.INFO)

# Set up client to master
master = ServerProxy('http://localhost:8000', allow_none=True)

df = pd.DataFrame()


# Functions
def read_csv(urlpath):
    global df
    df = pd.read_csv(urlpath)


def apply(func):
    return pickle.dumps(df.apply(eval(func)))


def columns():
    return pickle.dumps(df.columns)


def groupby(by):
    return pickle.dumps(df.groupby(by))


def head(n=5):
    return pickle.dumps(df.head(n))


def isin(values):
    return pickle.dumps(df.isin(values))


def items():
    """ list de tuples [label, contentSeries] """
    aux = ''
    for label, content in df.items():
        aux += f'label: {label}\n'
        aux += f'content:\n'
        for c in content:
            aux += f'{c}\n'
    return aux


def max(axis):
    return pickle.dumps(df.max(axis))


def min(axis):
    return pickle.dumps(df.min(axis))


worker.register_function(read_csv)
worker.register_function(apply)
worker.register_function(columns)
worker.register_function(groupby)
worker.register_function(head)
worker.register_function(isin)
worker.register_function(items)
worker.register_function(max)
worker.register_function(min)

# Start the server
try:
    print('Use Ctrl+c to exit')
    master.add_node('http://localhost:9000')
    worker.serve_forever()
except KeyboardInterrupt:
    master.remove_node('http://localhost:9000')
    print('Exiting')
