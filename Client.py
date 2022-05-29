import pickle
import threading
from time import sleep
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import pandas as pd

client = SimpleXMLRPCServer(('localhost', 10000), logRequests=True, allow_none=True)


def set_master(m_url):
    global master
    master = ServerProxy(m_url, allow_none=True)


def serve4ever():
    client.serve_forever()


client.register_function(set_master)

x = threading.Thread(target=serve4ever, daemon=True)
x.start()

# Communication with master
master = ServerProxy('http://localhost:8000', allow_none=True)

# Read CSV
csv_list = ["df1.csv", "df2.csv", "df3.csv"]

i = 0
for url in master.get_workers():
    print(url)
    worker = ServerProxy(url, allow_none=True)
    worker.read_csv(csv_list[i])
    i += 1

# Apply
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in master.get_workers():
    result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).apply("lambda x: x + 2").data)])
print(result)

# Columns
print("\nTest columns()")
result = pd.Index([])
for worker in master.get_workers():
    result = result.union(pickle.loads(ServerProxy(worker, allow_none=True).columns().data))
print(result)

print("all nodes are available...")
while len(master.get_workers()) == 3:
    sleep(30)
print("algun node s'ha removed")

# GroupBy
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
for worker in master.get_workers():
    result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).groupby('z').data).sum()])
print(result)

# Head
print("\nTest head()")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in master.get_workers():
    result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).head().data)])
print(result)

# isin
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in master.get_workers():
    result = pd.concat([result, pickle.loads(ServerProxy(worker, allow_none=True).isin([2, 4]).data)])
print(result)

# items
print("\nTest items")
result = ''
for worker in master.get_workers():
    result = result + "\n" + ServerProxy(worker, allow_none=True).items()
print(result)

# max
print("\nTest max(0)")
result = pd.Series([0, 0, 0], index=['x', 'y', 'z'])
for worker in master.get_workers():
    max_wk = pickle.loads(ServerProxy(worker, allow_none=True).max(0).data)
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
print(result)

# min
print("\nTest min(0)")
result = pd.Series([999, 999, 999], index=['x', 'y', 'z'])
for worker in master.get_workers():
    max_wk = pickle.loads(ServerProxy(worker, allow_none=True).min(0).data)
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
print(result)

print("\n\n*** Individual Tests ***\n\n")
for url in master.get_workers():
    worker = ServerProxy(url, allow_none=True)
    print("Test apply(lambda x: x + 2)")
    print(pickle.loads(worker.apply("lambda x: x + 2").data))
    print("Test columns()")
    print(pickle.loads(worker.columns().data))
    print("Test groupby(x).sum()")
    print(pickle.loads(worker.groupby('x').data).sum())
    print("Test head()")
    print(pickle.loads(worker.head().data))
    print("Test isin([2, 4])")
    print(pickle.loads(worker.isin([2, 4]).data))
    print("Test items()")
    print(worker.items())
    print("Test max(0)")
    print(pickle.loads(worker.max(0).data))
    print("Test min(0)")
    print(pickle.loads(worker.min(0).data))
