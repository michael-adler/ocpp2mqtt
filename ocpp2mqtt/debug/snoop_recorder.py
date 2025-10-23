#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import asyncio
import websockets
import logging
import json

def parse_args():
    """Parse command line arguments."""

    msg = """
Map a stream of OCPP messages to MQTT topics.
"""

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Record the OCPP snoop stream for playback later.",
        epilog=msg)

    parser.add_argument('--snoop-socket', type=str, default='ws://localhost:8501/',
        help="""URL of the OCPP relay's snoop port (default: %(default)s).""")

    parser.add_argument('-o', '--output', type=str, default='output.json',
        help="""Output file (default: %(default)s).""")

    global args
    args = parser.parse_args()

async def receive_forever():
    uri = args.snoop_socket
    print(f"Connecting to {uri}...")
    try:
        with open(args.output, "w", encoding="utf-8") as outfile:
            async with websockets.connect(uri) as websocket:
                print("Connection established. Waiting for messages...")
                async for message in websocket:
                    json_msg = json.loads(message)
                    print(f"Client receives: < {json.dumps(json_msg, indent=2)}")
                    outfile.write(json.dumps(json_msg) + "\n")
                    outfile.flush()
    except websockets.exceptions.ConnectionClosed as e:
        print(f"Connection closed: {e.code} ({e.reason})")
    except ConnectionRefusedError:
        print("Connection refused. Is the server running?")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def main():
    parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s')

    try:
        asyncio.run(receive_forever())
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == "__main__":
    main()
