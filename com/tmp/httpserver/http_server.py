from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from urllib.parse import urlparse
from urllib.parse import parse_qs
import json
# from urlparse import urlparse, parse_qs
from com.tmp.business.business_logic import BusinessLogic

businessObj = BusinessLogic()


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()

        url = self.path
        print(url)
        if not url.startswith('/dynamodb'):
            print('entered URL is not recognised')
            return

        parsed_url = urlparse(url)
        captured_value = parse_qs(parsed_url.query)
        print(captured_value)

        domain = captured_value['domain'][0]
        print("domain:", domain)

        table_name = "kafka_cache_table"
        attribute_key = "domain"
        attribute_value = domain

        kafka_cache_record = businessObj.get_kafka_cache(table_name, attribute_key, attribute_value)

        response = BytesIO()
        response.write(json.dumps(kafka_cache_record, indent=2).encode('utf-8'))
        self.wfile.write(response.getvalue())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.end_headers()
        url = self.path
        response = BytesIO()
        print("supplied url : ", url)
        print("supplied body : ", body)

        body = body.decode()
        body = body.replace("b'{", "{")
        body = body.replace("}'", "}")

        print("convert body to json format : ", body)

        payload = json.loads(body)

        if not url.startswith('/dynamodb'):
            print('entered URL is not recognised')
            return

        parsed_url = urlparse(url)
        captured_value = parse_qs(parsed_url.query)
        print(captured_value)

        domain = captured_value['domain'][0]
        print("domain:", domain)

        table_name = "kafka_cache_table"
        attribute_key = "domain"
        attribute_value = domain

        db_load_response = businessObj.populate_dynamodb(table_name, attribute_key, attribute_value, payload)
        print("db_load_response : ", db_load_response)

        response.write(json.dumps(db_load_response, indent=2).encode('utf-8'))
        self.wfile.write(response.getvalue())


httpd = HTTPServer(('localhost', 8000), SimpleHTTPRequestHandler)
httpd.serve_forever()
