#!/usr/bin/env python3
"""本机 TCP 转发，用于绕开只针对特定进程生效的网络策略。

    python3 tools/tcpforward.py 127.0.0.1:19001 <advertised-ip>:18001

然后把转发关系告诉 demo：

    DTS_BROKER_REWRITE=<advertised-ip>:18001=127.0.0.1:19001 go run .
"""
import socket
import sys
import threading


def parse(addr):
    host, _, port = addr.rpartition(":")
    return host, int(port)


def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def handle(downstream, target):
    try:
        upstream = socket.create_connection(target, 10)
    except OSError as err:
        print("upstream connect failed: %s" % err, flush=True)
        downstream.close()
        return
    threading.Thread(target=pipe, args=(downstream, upstream), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, downstream), daemon=True).start()


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: tcpforward.py <listen host:port> <target host:port>")
    listen, target = parse(sys.argv[1]), parse(sys.argv[2])

    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(listen)
    server.listen(64)
    print("forwarding %s:%d -> %s:%d" % (listen + target), flush=True)

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn, target), daemon=True).start()


if __name__ == "__main__":
    main()
