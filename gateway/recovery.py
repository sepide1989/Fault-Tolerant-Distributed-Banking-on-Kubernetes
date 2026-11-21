"""Dedicated crash-recovery process; no HTTP API or in-process SQLite state."""
import logging
import os
import time
from gateway.routing import Router
from gateway.coordinator_pg import Coordinator

logging.basicConfig(level=logging.INFO)
if __name__ == '__main__':
    coord = Coordinator(Router())
    while True:
        try:
            coord.recover_once()
        except Exception:
            logging.exception('coordinator recovery failed; will retry')
        time.sleep(float(os.getenv('RECOVERY_INTERVAL', '3')))
