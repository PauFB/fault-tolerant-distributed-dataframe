import pickle
import sys
import threading
from time import sleep
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import pandas

# Set up logging
client = SimpleXMLRPCServer(('localhost', 10000), logRequests=True, allow_none=True)


# Server functions
def set_master(m_url):
    global master
    master = ServerProxy(m_url, allow_none=True)


def serve_forever():
    client.serve_forever()


client.register_function(set_master)

# Start client server in parallel
x = threading.Thread(target=serve_forever, daemon=True)
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
        print("Some node has failed or has been removed.")
        sleep(5)

# apply()
print("\nTesting apply(lambda x: x + 2)")
finished = False
while not finished:
    try:
        result = pandas.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pandas.concat(
                [result, pickle.loads(ServerProxy(worker, allow_none=True).apply("lambda x: x + 2").data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# columns()
print("\nTesting columns()")
finished = False
while not finished:
    try:
        result = pandas.Index([])
        for worker in master.get_workers():
            result = result.union(pickle.loads(ServerProxy(worker, allow_none=True).columns().data))
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# groupby()
print("\nTesting groupby(z).sum()")
finished = False
while not finished:
    try:
        result = pandas.DataFrame(columns=['x', 'y'])
        for worker in master.get_workers():
            result = pandas.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).groupby('z').data).sum()])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# head()
print("\nTesting head()")
finished = False
while not finished:
    try:
        result = pandas.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pandas.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).head().data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# isin()
print("\nTesting isin([2, 4])")
finished = False
while not finished:
    try:
        result = pandas.DataFrame(columns=['x', 'y', 'z'])
        for worker in master.get_workers():
            result = pandas.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).isin([2, 4]).data)])
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# items()
print("\nTesting items()")
finished = False
while not finished:
    try:
        result = ''
        for worker in master.get_workers():
            result = result + "\n" + ServerProxy(worker, allow_none=True).items()
        print(result)
        finished = True
    except ConnectionError:
        print("Some node has failed or has been removed.")
        sleep(5)

# max()
print("\nTesting max(0)")
finished = False
while not finished:
    try:
        result = pandas.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
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
        print("Some node has failed or has been removed.")
        sleep(5)

# min()
print("\nTesting min(0)")
finished = False
while not finished:
    try:
        result = pandas.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
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
        print("Some node has failed or has been removed.")
        sleep(5)
