import logging
import pickle
import threading
import os
from time import sleep
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import pandas as pd

# Set up logging
self_url = 'http://localhost:9001'
worker = SimpleXMLRPCServer(('localhost', 9001), logRequests=True, allow_none=True)
logging.basicConfig(level=logging.INFO)

# Set up client to master
master_url = 'http://localhost:8000'
master = ServerProxy(master_url, allow_none=True)

workers_list = list()
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


# Functions
def add_node(w):
    workers_list.append(w)


def remove_node(w):
    workers_list.remove(w)


def get_workers():
    return workers_list


def set_workers(w_list):
    global workers_list
    workers_list = w_list


def check():
    return True


def get_master():
    return master_url


def set_master(url):
    global master_url
    master_url = url


def workers_fault_tolerance(url):
    w = ServerProxy(url, allow_none=True)
    try:
        w.check()
        print(url + " up")
    except ConnectionError:
        print(url + " down")
        workers_list.remove(url)


def master_fault_tolerance(url):
    m = ServerProxy(url, allow_none=True)
    global workers_list
    global master_url
    try:
        workers_list = m.get_workers()
        master_url = m.get_master()
        print(url + " up")
    except ConnectionError:
        print(url + " down")
        master_url = self_url
        workers_list.remove(self_url)
        for n in workers_list:
            ServerProxy(n, allow_none=True).set_master(self_url)
            ServerProxy(n, allow_none=True).set_workers(workers_list)


def serve4ever():
    worker.serve_forever()


worker.register_function(read_csv)
worker.register_function(apply)
worker.register_function(columns)
worker.register_function(groupby)
worker.register_function(head)
worker.register_function(isin)
worker.register_function(items)
worker.register_function(max)
worker.register_function(min)
worker.register_function(add_node)
worker.register_function(remove_node)
worker.register_function(get_workers)
worker.register_function(set_workers)
worker.register_function(check)
worker.register_function(get_master)
worker.register_function(set_master)

# Start the server
try:
    print('Use Ctrl+c to exit')
    x = threading.Thread(target=serve4ever, daemon=True)
    x.start()
    master.add_node(self_url)
    while True:
        if master_url == self_url:
            for node in workers_list:
                workers_fault_tolerance(node)
        else:
            master_fault_tolerance(master_url)
except KeyboardInterrupt:
    # master.remove_node(self_url)
    print('Exiting')
