import yaat, signal, sys

signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
if __name__ == '__main__':
    yaat.run()

# from http.server import HTTPServer, BaseHTTPRequestHandler

# class SimpleHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         self.send_response(200)
#         self.send_header("Content-type", "text/plain")
#         self.end_headers()
#         self.wfile.write(b"Hello, world from CICD#2!")

# import pymongo

# mongo


# if __name__ == '__main__':
#     server = HTTPServer(('0.0.0.0', 80), SimpleHandler)
#     print("Serving on http://0.0.0.0:80")
#     server.serve_forever(poll_interval=0.1)