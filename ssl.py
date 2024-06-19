import json
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
from google.auth import default
import urllib3

# Disable SSL warnings for testing purposes
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Patch google.auth.transport.requests.Request
class NoSSLRequest(Request):
    def __init__(self, *args, **kwargs):
        kwargs['verify'] = False
        super().__init__(*args, **kwargs)
