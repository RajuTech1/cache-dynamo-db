from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from urllib.parse import urlparse
from urllib.parse import parse_qs
#from urlparse import urlparse, parse_qs

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Hello, world!')

        url = self.path
        print(url)
        if not url.startswith('/db'):
            print('entered URL is not recognised')
            return

        parsed_url = urlparse(url)
        captured_value = parse_qs(parsed_url.query)
        print(captured_value)
        domain = captured_value['domain'][0]
        print(domain)
        print("done")




     #def do_POST(self):
     #   content_length = int(self.headers['Content-Length'])
    #    body = self.rfile.read(content_length)
     #   self.send_response(200)
     #   self.end_headers()
     #   response = BytesIO()
     #   response.write(b'This is POST request. ')
     #   response.write(b'Received: ')
     #   response.write(body)
     #   self.wfile.write(response.getvalue())-->


httpd = HTTPServer(('localhost', 8000), SimpleHTTPRequestHandler)
httpd.serve_forever()