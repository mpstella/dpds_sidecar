from dataclasses import dataclass, field
import subprocess
from pathlib import Path

import threading
import time
from typing import Any, Dict, List

import typer
import rpyc

from rpyc.utils.server import ThreadedServer

from dpds_sidecar import VERSION

app = typer.Typer(add_completion=False)

# we don't need fancy rich consoles for this
typer.core.rich = None

class GcsRsyncWorker(threading.Thread):

    MAX_ERROR_COUNT = 4

    def __init__(self, source, destination, rpc_server, sleep_time):

        threading.Thread.__init__(self)

        self.command = ["gsutil", "-m", "rsync", "-r", source, destination]
        
        self.sleep_time = sleep_time
        self.__rpc_server = rpc_server
        self.__stop_event = threading.Event()

    def stop(self):
        self.__stop_event.set()
        print("Signalling RPC server to stop ...")
        self.__rpc_server.stop()

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

@dataclass
class TestObject:
    fqn: str
    ports: List[str] = field(default_factory=list)


class TestObject2:

    def greeting(self) -> str:
        return f"Hello from {self}"

class DpdsService(rpyc.Service):

    ALIASES = ["dpds_retrieval"]

    def __init__(self, source):
        self.source = Path(source)

    def exposed_get(self, fqn: str) -> Dict[str, Any]:

        p = self.source / fqn

        print(f"Searching for {p} ..")

        if p.exists():
            with open(p, 'r') as file:
                return file.read()
        
        print("Not found, fetching from DPC API ...")
        # add code here to fetch from DPDS

        raise FileNotFoundError

    
    def exposed_get_obj(self) -> TestObject:
        return TestObject("abc_fqn", ["port1", "port2"])
    
    def exposed_get_obj2(self) -> TestObject2:
        return TestObject2()
    
class CustomThreadedService(ThreadedServer):
    def __init__(self, service, port, source, **kwargs):
        super().__init__(service(source), port=port, **kwargs)

    def stop(self):
        print("Stopping RPC Server ...")
        self.close()

@app.command()
def version() -> None:
    typer.echo(f"Version: {VERSION}")

@app.command()
def start(
    source: str = typer.Option(..., "--source", "-s", help="Source location (GCS Bucket + 'folder')"), 
    destination: str = typer.Option(..., "--destination", "-d", help="Destination location"), 
    sleep_time: int = typer.Option(60, "--sleep", "-x", help="Sleep duration between synchronising"),
    port: int = typer.Option(12345, "--port", "-p", help="RPC port for server to bind")
) -> None:

    p = Path(destination)

    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

    server = CustomThreadedService(DpdsService, 
                                   port=port, 
                                   source=destination, 
                                   protocol_config={'allow_public_attrs': True})
    sync = GcsRsyncWorker(source, destination, server, sleep_time)

    print(">> Starting GCS Sync Service ... <<")
    sync.start()
    print(">> Starting DPDS Service ... <<")
    server.start()

    sync.join()

    print("Stopped.")

if __name__ == "__main__":
    app.run()
