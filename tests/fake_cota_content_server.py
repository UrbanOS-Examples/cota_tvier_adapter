import http.server
import socketserver
from os import path, getcwd
from threading import Thread, Lock

def _simple_server(port=8000):
  handler = http.server.SimpleHTTPRequestHandler
  with socketserver.TCPServer(("127.0.0.1", port), handler) as server:
      print("Server started at localhost:" + str(port))
      server.serve_forever()


def create_fake_server(port=8000, daemon=True):
  Thread(target=_simple_server, args=[port], daemon=daemon).start()

  return f"http://localhost:8000/tests/fixtures"


if __name__ == "__main__":
    create_fake_server(daemon=False)