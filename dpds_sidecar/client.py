import sys
import rpyc

def run(port: int, fqn: str) -> None:
    
    conn = rpyc.connect("localhost", port)

    f = conn.root.get(fqn)
    print(f)
    conn.close()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python client.py <port> <fqn>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2])