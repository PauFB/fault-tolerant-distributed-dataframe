import logging
import pickle
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import pandas


class Worker:
    def __init__(self, port, csv_path):
        # Set up logging
        worker = SimpleXMLRPCServer(('localhost', port), logRequests=True, allow_none=True)
        logging.basicConfig(level=logging.INFO)

        # Priority magnitude for master election
        self.priority = port

        # URLs
        self.self_url = 'http://localhost:' + str(port)
        self.client_url = 'http://localhost:10000'
        self.master_url = 'http://localhost:8000'

        # Set up client to master
        master = ServerProxy(self.master_url, allow_none=True)

        self.workers_list = list()
        self.df = pandas.read_csv(csv_path)

        # Server functions
        def read_csv(urlpath):
            self.df = pandas.read_csv(urlpath)

        def apply(func):
            return pickle.dumps(self.df.apply(eval(func)))

        def columns():
            return pickle.dumps(self.df.columns)

        def groupby(by):
            return pickle.dumps(self.df.groupby(by))

        def head(n=5):
            return pickle.dumps(self.df.head(n))

        def isin(values):
            return pickle.dumps(self.df.isin(values))

        def items():
            aux = ''
            for label, content in self.df.items():
                aux += f'label: {label}\n'
                aux += f'content:\n'
                for c in content:
                    aux += f'{c}\n'
            return aux

        def max(axis):
            return pickle.dumps(self.df.max(axis))

        def min(axis):
            return pickle.dumps(self.df.min(axis))

        def add_node(worker_url):
            self.workers_list.append(worker_url)

        def remove_node(worker_url):
            self.workers_list.remove(worker_url)

        def get_workers():
            return self.workers_list

        def set_workers(workers_list):
            self.workers_list = workers_list

        def check():
            return True

        def get_priority():
            return self.priority

        def set_master(url):
            self.master_url = url

        def check_worker_availabilities():
            for url in self.workers_list:
                w = ServerProxy(url, allow_none=True)
                try:
                    w.check()
                    print(url + " up")
                except ConnectionError:
                    print(url + " down")
                    # Remove any unreachable node that causes a connection error
                    self.workers_list.remove(url)

        def check_master_availability():
            m = ServerProxy(self.master_url, allow_none=True)
            try:
                self.workers_list = m.get_workers()
                print(self.master_url + " up")
            except ConnectionError:
                print(self.master_url + " down")
                become_master = True
                for worker_url in self.workers_list:  # For all others workers
                    if worker_url != self.self_url and worker_url != self.master_url:
                        # If another worker has a higher priority magnitude do not become master
                        if (ServerProxy(worker_url, allow_none=True).get_priority()) > self.priority:
                            become_master = False
                            break
                if become_master:
                    self.master_url = self.self_url
                    ServerProxy(self.client_url, allow_none=True).set_master(self.self_url)  # Notify the client
                    # Notify all workers except myself and the recently dead master
                    for worker_url in self.workers_list:
                        if worker_url != self.self_url and worker_url != self.master_url:
                            ServerProxy(worker_url, allow_none=True).set_master(self.self_url)

        def serve_forever():
            worker.serve_forever()

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
        worker.register_function(read_csv)
        worker.register_function(check)
        worker.register_function(get_priority)
        worker.register_function(get_workers)
        worker.register_function(set_master)
        worker.register_function(set_workers)

        # Start the server
        try:
            print('Use Ctrl+c to remove from cluster')

            # Start server in parallel
            x = threading.Thread(target=serve_forever, daemon=True)
            x.start()

            # Add worker to the cluster
            master.add_node(self.self_url)

            # Fault tolerance
            while True:
                if self.master_url == self.self_url:
                    check_worker_availabilities()
                else:
                    check_master_availability()
        except KeyboardInterrupt:
            master.remove_node(self.self_url)
            print('Exiting')
