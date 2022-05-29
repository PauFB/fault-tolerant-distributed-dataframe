import pickle
import threading
from time import sleep
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import pandas as pd

# Set up logging
client = SimpleXMLRPCServer(('localhost', 10000), logRequests=True, allow_none=True)


# Functions
def set_master(m_url):
    global master
    master = ServerProxy(m_url, allow_none=True)


def serve4ever():
    client.serve_forever()


client.register_function(set_master)

# Start client in parallel
x = threading.Thread(target=serve4ever, daemon=True)
x.start()

# Set up client to master
master = ServerProxy('http://localhost:8000', allow_none=True)

# CSV
csv_list = ["df1.csv", "df2.csv", "df3.csv"]

finished = False
while not finished:
    try:
        i = 0
        for url in master.get_workers():
            print(url)
            worker = ServerProxy(url, allow_none=True)
            worker.read_csv(csv_list[i])
            i += 1
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)

# Apply
print("\nTest apply(lambda x: x + 2)")
finished = False
while not finished:
    try:
        result = pd.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).apply("lambda x: x + 2").data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# Columns
print("\nTest columns()")
finished = False
while not finished:
    try:
        result = pd.Index([])
        for worker in master.get_workers():
            result = result.union(pickle.loads(ServerProxy(worker, allow_none=True).columns().data))
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# GroupBy
print("\nTest groupby(z).sum()")
finished = False
while not finished:
    try:
        result = pd.DataFrame(columns=['x', 'y'])
        for worker in master.get_workers():
            result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).groupby('z').data).sum()])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# Head
print("\nTest head()")
finished = False
while not finished:
    try:
        result = pd.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).head().data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# isin
print("\nTest isin([2, 4])")
finished = False
while not finished:
    try:
        result = pd.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).isin([2, 4]).data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# items
print("\nTest items")
finished = False
while not finished:
    try:
        result = ''
        for worker in master.get_workers():
            result = result + "\n" + ServerProxy(worker, allow_none=True).items()
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# max
print("\nTest max(0)")
finished = False
while not finished:
    try:
        result = pd.Series([0, 0, 0], index=['x', 'y', 'z'])
        for worker in master.get_workers():
            max_wk = pickle.loads(ServerProxy(worker, allow_none=True).max(0).data)
            i = 0
            for index, value in max_wk.items():
                if value > result[i]:
                    result[i] = value
                i += 1
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)


# min
print("\nTest min(0)")
finished = False
while not finished:
    try:
        result = pd.Series([999, 999, 999], index=['x', 'y', 'z'])
        for worker in master.get_workers():
            max_wk = pickle.loads(ServerProxy(worker, allow_none=True).min(0).data)
            i = 0
            for index, value in max_wk.items():
                if value < result[i]:
                    result[i] = value
                i += 1
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or removed")
        sleep(5)
