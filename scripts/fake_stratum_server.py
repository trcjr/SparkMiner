#!/usr/bin/env python3

import argparse
import json
import socket
import threading
import time


SUBSCRIBE_RESULT = {
    "id": 1,
    "result": [[[
        "mining.set_difficulty",
        "deadbeef"
    ]], "abcd1234", 4],
    "error": None,
}

AUTHORIZE_RESULT = {"id": 3, "result": True, "error": None}
SET_DIFFICULTY = {"id": None, "method": "mining.set_difficulty", "params": [0.0014]}


def make_notify(job_id, prevhash, clean_jobs):
    return {
        "id": None,
        "method": "mining.notify",
        "params": [
            job_id,
            prevhash,
            "0100000001",
            "ffffffff",
            [],
            "20000000",
            "1d00ffff",
            "66000000",
            clean_jobs,
        ],
    }


class FakeStratumServer:
    def __init__(self, host, port, scenario, notify_delay, clean_jobs):
        self.host = host
        self.port = port
        self.scenario = scenario
        self.notify_delay = notify_delay
        self.clean_jobs = clean_jobs
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def serve(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        print(f"[FAKE-STRATUM] listening on {self.host}:{self.port} scenario={self.scenario}")
        conn, addr = self.server_socket.accept()
        print(f"[FAKE-STRATUM] client connected from {addr[0]}:{addr[1]}")
        try:
            self.handle_client(conn)
        finally:
            conn.close()
            self.server_socket.close()

    def send_json(self, conn, payload):
        wire = json.dumps(payload, separators=(",", ":")) + "\n"
        conn.sendall(wire.encode("utf-8"))
        print(f"[FAKE-STRATUM] TX {wire.strip()}")

    def maybe_send_followup_notify(self, conn):
        if self.scenario == "omit-notify":
            print("[FAKE-STRATUM] omit-notify scenario: not sending follow-up mining.notify")
            return

        if self.notify_delay > 0:
            print(f"[FAKE-STRATUM] delaying follow-up notify by {self.notify_delay:.1f}s")
            time.sleep(self.notify_delay)

        prevhash = "22" * 32 if self.scenario != "same-prevhash" else "11" * 32
        self.send_json(conn, make_notify("job-2", prevhash, self.clean_jobs))

    def handle_client(self, conn):
        self.send_json(conn, SUBSCRIBE_RESULT)
        self.send_json(conn, SET_DIFFICULTY)
        self.send_json(conn, AUTHORIZE_RESULT)
        self.send_json(conn, make_notify("job-1", "11" * 32, False))

        buffer = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                print("[FAKE-STRATUM] client disconnected")
                return
            buffer += chunk
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                print(f"[FAKE-STRATUM] RX {line}")
                msg = json.loads(line)
                method = msg.get("method")
                if method == "mining.submit":
                    self.send_json(conn, {"id": msg.get("id"), "result": True, "error": None})
                    threading.Thread(target=self.maybe_send_followup_notify, args=(conn,), daemon=True).start()
                elif method == "mining.suggest_difficulty":
                    self.send_json(conn, {"id": msg.get("id"), "result": True, "error": None})
                elif method == "mining.subscribe":
                    self.send_json(conn, {"id": msg.get("id"), "result": SUBSCRIBE_RESULT["result"], "error": None})
                elif method == "mining.authorize":
                    self.send_json(conn, {"id": msg.get("id"), "result": True, "error": None})


def parse_args():
    parser = argparse.ArgumentParser(description="Minimal fake Stratum server for post-block notify testing")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=3333)
    parser.add_argument(
        "--scenario",
        choices=["delay-notify", "omit-notify", "same-prevhash"],
        default="delay-notify",
    )
    parser.add_argument("--notify-delay", type=float, default=65.0)
    parser.add_argument("--clean-jobs", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    server = FakeStratumServer(
        host=args.host,
        port=args.port,
        scenario=args.scenario,
        notify_delay=args.notify_delay,
        clean_jobs=args.clean_jobs,
    )
    server.serve()


if __name__ == "__main__":
    main()