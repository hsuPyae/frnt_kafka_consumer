import os
import io
import time

from socketserver import ThreadingMixIn
from http.server import HTTPServer, BaseHTTPRequestHandler

from. import http_helper
from consumer_log import get_logging

def read_html() -> str:
    _default_html = ""
    with open("html/default.html", "r") as read_html:
        _default_html = read_html.read()
    return _default_html

DEFAULT_HTML = read_html()

class WebThreadServer(ThreadingMixIn, HTTPServer):
    pass

def home(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header('Content-type','text/html')
    handler.end_headers()
    status = http_helper.create_status_html()
    status_header = http_helper.status_header_html()
    title = "Consumer Home"
    data = f"""
    <div class="container header">
        <div class="flex-container">
        {status_header}
        </div>
    </div>
    {status}
    """
    html = DEFAULT_HTML.replace("TITLE_HERE", title)
    html = html.replace("DATA_HERE", data)
    handler.wfile.write(html.encode('utf-8'))

def log_page(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header('Content-type','text/html')
    handler.end_headers()
    title = "Log Page"
    data = http_helper.get_log_paths(http_helper.LOG_PATH, url=handler.path)
    html = DEFAULT_HTML.replace("TITLE_HERE", title)
    html = html.replace("DATA_HERE", data)
    handler.wfile.write(html.encode('utf-8'))

def download_log_page(handler: BaseHTTPRequestHandler) -> None:
    filename = http_helper.parse_field(handler.path, "fname")
    if not filename:
        return
    filename = filename[0]
    handler.send_response(200)
    handler.send_header('Content-type', 'application/log')
    handler.send_header('Content-Disposition', f"""attachment; filename="{filename}" """)
    handler.end_headers()
    with open(os.path.join(http_helper.LOG_PATH, filename), 'rb') as wf:
        handler.wfile.write(wf.read())

def tail_page(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header('Content-type','text/html')
    handler.end_headers()
    title = "Tail Log"
    data = """
    """
    index = DEFAULT_HTML.index("DATA_HERE")
    html = DEFAULT_HTML[:index]
    html = html.replace("TITLE_HERE", title)
    html = html.replace("DATA_HERE", data)
    handler.wfile.write(html.encode('utf-8'))
    fname = http_helper.parse_field(handler.path, "fname")
    if not fname:
        return handler.wfile.write("No param fname in url!".encode('utf-8'))
    http_helper.tail_log(handler, fname[0])

def add_or_update_topic(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header("Content-type", "text/html")
    handler.end_headers()
    fname = http_helper.parse_field(handler.path, "topic") or None
    if not fname and handler.path.startswith("/update"):
        return handler.wfile.write("No param fname in url!".encode('utf-8'))
    if fname:
        fname = fname[0]
    html = DEFAULT_HTML.replace("DATA_HERE", http_helper.get_form_html(handler, fname))
    handler.wfile.write(html.encode('utf-8'))

def default_page(handler: BaseHTTPRequestHandler) -> None:
    handler.send_response(200)
    handler.send_header("Content-type", "text/html")
    handler.end_headers()
    handler.wfile.write("Not Supported Route".encode('utf-8'))

class HandleRequests(BaseHTTPRequestHandler):
    """
    Handler Class For ALL Requests.
    """
    def do_GET(self):
        if self.path == "/" or self.path == "/home":
            home(self)
        elif self.path == "/logs" or self.path == "/logs/more":
            log_page(self)
        elif self.path.startswith("/logs/download"):
            download_log_page(self)
        elif self.path.startswith("/tail"):
            tail_page(self)
        elif self.path.startswith("/create") or self.path.startswith("/update"):
            add_or_update_topic(self)
        elif self.path.startswith("/delete"):
            if http_helper.handle_delete_topic(self):
                self.send_response(302)
                self.send_header('Location', "/")
                self.end_headers()
        else:
            default_page(self)

    def do_POST(self):
        if self.path.startswith("/create"):
            if http_helper.handle_topic_post_request(self):
                self.send_response(302)
                self.send_header('Location', "/")
                self.end_headers()

def start():
    with WebThreadServer(('', http_helper.PORT), HandleRequests) as web_server:
        web_server.serve_forever()

if __name__ == "__main__":
    start()
