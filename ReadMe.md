# Kafka Consumer

## Configuration

### Server Config

```
#Kfaka server setting
[kafka]
settings = {'bootstrap_servers':['0.0.0.0:9092'],'auto_offset_reset':'earliest','enable_auto_commit': False,'asks_arg':1,'poll_timeout_sec':1,'consumer_timeout_sec':5,'security_protocol':'SASL_PLAINTEXT','sasl_mechanism':'PLAIN'}

# Log setting server
[logging]
path = /var/log/consumer/
max_bytes = 1024
backup_count = 0
[webui]
port = 8000
host = loalhost

# Odoo RPC setting
[xmlrpc]
url = http://{}:{}
obj_rpc = {}/xmlrpc/2/object
obj_common = {}/xmlrpc/2/common

# Odoo Server setting
[odoo]
odoo_server = localhost
odoo_port = 8069
odoo_db = qac13
odoo_user = admin
odoo_password = admin
```

### Topic Config
```
[topic_name]
topic = topic_name                               # Kafka Topic Name
group_id = erpdev                                # Group ID
endpoint = http://127.0.0.1:8069/v1/consumer     # API Endpoint that will receive kafka data
timeout = 3                                      # Timeout to call endpoint
max_records = 1000                               # Maximun record at a time to consume
username = admin                                 # Kafka username
password = admin-secret                          # Kafka secret
active = True                                    # Enable or Not
auth_header = API_KEY                            # HEADER for API Endpoint eg: "Authorization"
token = frontiir                                 # Token fo API Endpoint eg: "Bearer 13242424"
is_rpc = True                                    # is_rpc True (It is odoo specific and Set False in other system)
odoo_model = data.sync                           # (Odoo specific field and leave blank with key eg: odoo_model=blank)
odoo_method = rpc_consumer                       # (Odoo specific field and leave blank with key eg: odoo_method=blank)
```

## Start the service

### Using docker
>docker compose up

### Using python
Python version : 3.9

#### Install Packages
>pip install -r requirements.txt

#### Run
>python server.py
