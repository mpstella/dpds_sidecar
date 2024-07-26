import sys
import rpyc

def run(fqn: str) -> None:
    
    conn = rpyc.connect("localhost", 12345)

    f = conn.root.get(fqn)
    print(f)
    conn.close()


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("Usage: python client.py <fqn>")
        sys.exit(1)

    run(sys.argv[1])