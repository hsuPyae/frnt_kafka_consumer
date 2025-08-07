import os
import io
import time
import json
import configparser
import html
from http.server import BaseHTTPRequestHandler

from urllib.parse import urlparse,parse_qs
from collections import namedtuple

from kafka_config import DefaultConfigFile, KafkaConfig, ConfigConst
PS_LOG_FILE = "ps_status.log"

kafka_server_config = KafkaConfig()

LOG_PATH = kafka_server_config.get_config().get(ConfigConst.PATH)
PORT = kafka_server_config.get_config().get(ConfigConst.PORT)

class HttpConfig:
    """
    Config Reader
    """
    def __init__(self) -> None:
        self.config = configparser.ConfigParser()
        self.config_path = f"config/{DefaultConfigFile.TOPIC_FILE_NAME}"
        self.config.read(self.config_path)
        self.topic_dict = {}
        for topic in self.config.sections():
            tmp_data = {}
            for option in self.config.options(topic):
                tmp_data.update({
                    option:self.config.get(topic,option)
                })
            self.topic_dict.update(
                {topic:tmp_data}
            )

    def update_config(self,section: str, option: str, value: str)->None:
        with open(self.config_path, "w") as write_file:
            self.config.set(section, option, value)
            self.config.write(write_file)

StatusFields = namedtuple("StatusFields", ["group_id", "username", "endpoint", "timeout", "max_records", "active", "status", "pid"])

def read_log(file_buffer: io.TextIOWrapper) -> str:
    output = ""
    while True:
        line = file_buffer.readline()
        if line:
            output += f"""<span class="log-line">{change_log_color(html.escape(line))}</span></br>"""
        else:
            break
    return output

def tail_log(handler: BaseHTTPRequestHandler, fname: str) -> None:
    _log_path = os.path.join(LOG_PATH, fname)
    if not os.path.exists(_log_path):
        handler.wfile.write(f"<h3 class='error'>No log file [{_log_path}] found!</h3>".encode('utf-8'))
        return
    file_buffer = open(_log_path, "r")
    file_buffer.seek(os.path.getsize(_log_path))
    handler.wfile.write(read_log(file_buffer=file_buffer).encode('utf-8'))
    while True:
        try:
            lines = read_log(file_buffer=file_buffer).encode('utf-8')
            if lines:
                handler.wfile.write(lines)
        except BrokenPipeError:
            print("Client closed the connection")
            break
        time.sleep(0.5)
    file_buffer.close()

def process_status_reader() -> dict:
    output = None
    ps_log_file_path = os.path.join(LOG_PATH, PS_LOG_FILE)
    if not os.path.exists(ps_log_file_path):
        return {}
    with open(ps_log_file_path, "r") as read_file:
        data = read_file.readline()
        output = json.loads(data)
    return output

def get_log_paths(path: str, url: str) -> str:
    output = ""
    more_logs = ""
    files = []
    if url == "/logs":
        files = [ f for f in os.listdir(path=path) if os.path.isfile(os.path.join(path,f)) and f.endswith(".log") ]
        more_logs += f"""
        <li class="log-file"><a href="/logs/more" style="color: green;">More Logs</a></li>
        """
    else:
        files = [ f for f in os.listdir(path=path) if os.path.isfile(os.path.join(path,f)) and not f.endswith(".log") ]
        more_logs += f"""
        <li class="log-file"><a href="/logs" style="color: green;"><-Back</a></li>
        """
    files.sort()
    for file in files:
        output += f"""
        <li class="log-file">
Click to tail <a href="/tail?fname={file}">{file}</a>    <a href="/logs/download?fname={file}" class="info">Download</a>
        </li>
        """
    output += more_logs
    return "<ul>" + output + "</ul>"

def parse_field(path: str, field: str)-> str:
    try:
        return parse_qs(urlparse(path).query)[field]
    except KeyError:
        return []

def change_log_color(log: str)-> str:
    log = log.replace("INFO","""<span class="info">INFO</span>""")
    log = log.replace("ERROR","""<span class="error">ERROR</span>""")
    log = log.replace("WARNING","""<span class="warning">WARNING</span>""")
    return log

def status_field_html(data: dict, topic: str)->str:
    topic_running_status = process_status_reader()
    output_html = f"""
    <div class="topic-field topic-header"><a class="button-a" href="/tail?fname={topic}.log">{topic}</a></div>
    """
    for key in StatusFields._fields:
        if key == "endpoint":
            output_html += f"""
            <div class="topic-field topic-field-endpoint {key} {topic}">{data.get(key)}</div>
            """
            continue
        if key == "status":
            value = '<span class="info">Running</span>' if topic in topic_running_status else '<span class="error">Stopped</span>'
            output_html += f"""
                <div class="topic-field {key} {topic}">{value}</div>
                """
            continue
        if key == "pid":
            value = topic_running_status.get(topic) if topic in topic_running_status else None
            output_html += f"""
                <div class="topic-field {key} {topic}">{value}</div>
                """
            continue
        output_html += f"""
            <div class="topic-field {key} {topic}">{data.get(key)}</div>
            """
    #  <div class="topic-field edit-field"><a href="/update?topic={topic}">Edit</a> <a href="/delete?topic={topic}">Delete</a></div>
    output_html +=f"""
    <div class="topic-field edit-field"></div>
    """
    return output_html

def status_header_html()->str:
    output_html = f"""
    <div class="topic-field topic-field-header">TOPIC</div>
    """
    for key in StatusFields._fields:
        if key =="endpoint":
            output_html+=f"""
            <div class="topic-field topic-field-endpoint {key} topic-field-header">{key.upper()}</div>
            """
            continue
        output_html+=f"""
        <div class="topic-field topic-field-header {key}">{key.upper()}</div>
        """
    output_html+=f"""
    <div class="topic-field topic-field-header edit-field"></div>
    """
    return output_html

def create_status_html()->str:
    config = HttpConfig()
    output_html = ""
    for topic in config.topic_dict:
        output_html += f"""
        <div class="container">
            <div class="flex-container">{status_field_html(config.topic_dict.get(topic), topic)}</div>
        </div>
        """
    return output_html

def selection_res(val: str)->str:
    if val == "True":
        return """
        <option value='True'>True</option>
        <option value='False'>False</option>
        """
    else:
        return """
        <option value='False'>False</option>
        <option value='True'>True</option>
        """

def get_form_html(handler: BaseHTTPRequestHandler, topic: str)->str:
    output = ""
    config = HttpConfig()
    current_topic = config.topic_dict.get(topic) if topic else None
    if topic and not current_topic:
        return f"No such topic{topic} in settings!"
    output += f"""
    <label for="topic">topic:</label><br>
    <input type="text" id="topic" name="topic" value="{topic if topic else ""}" required><br>
    """
    for field in StatusFields._fields:
        if field == "status" or field == "pid":
            continue
        if field == "active":
            output+=f"""
            <label for="{field}">{field}:</label>
            <select id="{field}" name="{field}">
            {selection_res(current_topic.get(field) if current_topic else 'True')}
            </select><br>
            """
            continue
        if field == "timeout" or field == "max_records":
            output+=f"""
                <label for="{field}">{field}:</label><br>
                <input type="number" id="{field}" name="{field}" value="{current_topic.get(field) if topic else ""}" required><br>
                """
            continue
        output+=f"""
        <label for="{field}">{field}:</label><br>
        <input type="text" id="{field}" name="{field}" value="{current_topic.get(field) if topic else ""}" required><br>
        """
    return f"""<form action="/create?topic={topic if topic else ""}" method="POST">
    {output}
    <input type="submit">
    </form>"""

def handle_topic_post_request(handler: BaseHTTPRequestHandler)-> bool:
    output = False
    request_length = int(handler.headers["Content-Length"])
    data = "?"+handler.rfile.read(request_length).decode('utf-8')
    config = HttpConfig()
    current_topic = parse_field(handler.path, "topic")[0] if parse_field(handler.path, "topic") else None
    topic = parse_field(data, "topic")[0] if parse_field(data, "topic") else None
    if not topic:
        handler.wfile.write("<div>No Topic</div>".encode('utf-8'))
        return output
    config.config.remove_section(current_topic)
    config.config.add_section(topic)
    for field in StatusFields._fields:
        if field == "status" or field == "pid":
            continue
        tmp_data = parse_field(data, field)[0] if parse_field(data, field) else ""
        config.config.set(topic, field, tmp_data)
    try:
        with open(config.config_path, "w") as wf:
            config.config.write(wf)
        output = True
    except IOError as err:
        print(err)
        output = False
    return output

def handle_delete_topic(handler: BaseHTTPRequestHandler) -> bool:
    output = False
    topic = parse_field(handler.path, "topic")[0] if parse_field(handler.path, "topic") else None
    if not topic:
        return output
    config = HttpConfig()
    config.config.remove_section(topic)
    try:
        with open(config.config_path, "w") as wf:
            config.config.write(wf)
        output = True
    except IOError as err:
        print(err)
        output = False
    return output
