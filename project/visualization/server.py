import http.server
import socketserver

PORT = 8001
Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as http:
    print("serving at port", PORT)
    http.serve_forever()