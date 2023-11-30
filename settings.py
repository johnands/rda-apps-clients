import os

SLEEP_INTERVAL = int(os.environ.get("SLEEP_INTERVAL", 60*5))
LOGLEVEL = os.environ.get("ML_LOGLEVEL", "DEBUG")