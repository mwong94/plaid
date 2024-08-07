from datetime import date, datetime
from time import sleep
import json

class RateLimiter():
    def __init__(self, min_time:int):
        self.min_time = min_time
    def __enter__(self):
        self.start = datetime.utcnow()
    def __exit__(self, exception_type, exception_value, exception_traceback):
        duration = (datetime.utcnow() - self.start).total_seconds()
        sleep_time = max(0, self.min_time - duration)
        sleep(sleep_time)


def cast_to_string(v):
    if isinstance(v, list|dict):
        return json.dumps(v)
    else:
        return v


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
