import json
import urllib3
from google.auth.transport.requests import Request  # Import Request class from google.auth.transport.requests

# Disable SSL warnings for testing purposes
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NoSSLRequest(Request):
    def __init__(self, *args, **kwargs):
        kwargs['verify'] = False
        super().__init__(*args, **kwargs)
