import os
import subprocess

import threading
import time
from typing import Any, Dict

import typer
import rpyc

from rpyc.utils.server import ThreadedServer

app = typer.Typer(add_completion=False)

class GcsRsyncWorker(threading.Thread):

    MAX_ERROR_COUNT = 4
    DEFAULT_SLEEP_TIME = 60 * 10

    def __init__(self, source, destination, rpc_server, sleep_time=DEFAULT_SLEEP_TIME):

        threading.Thread.__init__(self)

        self.command = ["gsutil", "-m", "rsync", "-r", source, destination]
        
        self.rpc_server = rpc_server

        self.sleep_time = sleep_time
        self.__stop_event = threading.Event()

    def stop(self):
        self.__stop_event.set()
        print("Signalling RPC server to stop ...")
        self.rpc_server.stop()

    @property
    def stopped(self):
        return self.__stop_event.is_set()

    def run(self) -> None:

        error_count = 1

        while not self.stopped:
            try:
                print("** synching GCS ...")
                subprocess.run(self.command, check=True)
                error_count = 0
            except subprocess.CalledProcessError as e:
                print(f"Error ({error_count} / {GcsRsyncWorker.MAX_ERROR_COUNT}) orccurred: {e}")
                error_count += 1

                if error_count >= GcsRsyncWorker.MAX_ERROR_COUNT:
                    self.stop()
            
            time.sleep(self.sleep_time)


class DpdsService(rpyc.Service):

    def __init__(self, source):
        self.source = source

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass

    def exposed_get(self, fqn: str) -> Dict[str, Any]:

        f = os.path.join(self.source, fqn)

        print(f"Searching for {f} ..")

        if os.path.exists(f):
            with open(f, 'r') as file:
                return file.read()
        
        print("Not found, fetching from DPC API ...")
        return {"path": "__NOT__FOUND__"}
    
class CustomThreadedService(ThreadedServer):
    def __init__(self, service, port, source, **kwargs):
        super().__init__(service(source), port=port, **kwargs)

    def stop(self):
        print("Stopping RPC Server ...")
        self.close()

@app.command()
def start(source: str, 
          destination: str, 
          sleep_time: int,
          port: int = 12345
          ) -> None:

    server = CustomThreadedService(DpdsService, port=port, source=destination)
    sync = GcsRsyncWorker(source, destination, server, sleep_time)

    print(">> Starting GCS Sync Service ... <<")
    sync.start()
    print(">> Starting DPDS Service ... <<")
    server.start()

    sync.join()

    print("Stopped.")

if __name__ == "__main__":
    app.run()
