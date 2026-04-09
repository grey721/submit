#!/usr/bin/env python3
import datetime
import os
import socket
import sys
import time


def main() -> int:
    interval = 1.0
    if len(sys.argv) > 1:
        try:
            interval = max(0.1, float(sys.argv[1]))
        except ValueError:
            pass

    pid = os.getpid()
    host = socket.gethostname()
    i = 0
    while True:
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{now}] loop={i} host={host} pid={pid} cpu-only=true", flush=True)
                i += 1
                time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
