import sys
from typing import Any
import rpyc

from pprint import pprint

# from rpyc.core.protocol import ConnectionRefusedError



def run(conn: Any, fqn: str) -> None:
    
    try:
        f = conn.root.get(fqn)
        print(f)
    except FileNotFoundError:
        print("Looks like we couldn't get the dpds")    

def run2(conn: Any) -> None:

    f = conn.root.get_obj()
    print(f"fqn: {f.fqn}")
    pprint(f.ports)
    pprint(f)

    f = conn.root.get_obj2()
    pprint(f)
    print(f.greeting())

if __name__ == "__main__":

    argc = len(sys.argv)

    if argc <= 1:
        print("Usage: python client.py <port> <fqn>")
        sys.exit(1)

    conn = rpyc.connect("localhost", sys.argv[1])

    print(f"Connected to: {conn.root.get_service_name()}")

    if len(sys.argv) == 3:
        run(conn, sys.argv[2])
    else:
        run2(conn)  

    conn.close()