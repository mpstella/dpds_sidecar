import subprocess
from pathlib import Path

import threading
import time
from typing import Any, Dict

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


class DpdsService(rpyc.Service):

    def __init__(self, source):
        self.source = Path(source)

    def exposed_get(self, fqn: str) -> Dict[str, Any]:

        p = self.source / fqn

        print(f"Searching for {p} ..")

        if p.exists():
            with open(p, 'r') as file:
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
