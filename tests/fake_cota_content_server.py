import http.server
import socket
import socketserver
import sys
from os import getcwd, path
from threading import Lock, Thread


class StoppableTCPServer(socketserver.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)


def create_fake_server(port=8000, daemon=True):
    try:
        handler = http.server.SimpleHTTPRequestHandler
        server = StoppableTCPServer(("127.0.0.1", port), handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        print("Server started at localhost:" + str(port))
        if not daemon:
            thread.join()

        return f"http://localhost:8000/tests/fixtures"
    except (KeyboardInterrupt, SystemExit):
        print("Interrupted")
        server.server_close()
        # server.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    create_fake_server(daemon=False)
