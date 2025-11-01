#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import logging
import ssl
import sys
import yaml

from ocpp2mqtt.relay.ocpprelay import OCPPRelay
from ocpp2mqtt.relay.snoopws import SnoopWebSocketServer

def parse_args():
    """Parse command line arguments."""

    msg = """
The relay "implements" OCPP by blindly forwarding messages between a charge point
and a CPMS. Charge points must be configured to connect to the relay and the relay
must be configured with the URL of the real CPMS that would normally be used by the
charge point.

The relay acts as a WebSocket server with two ports: one for charge points and one for
snoop clients. The snoop port allows clients to connect and receive a copy of all
messages exchanged between charge points and the CPMS. A snoop client is provided
that maps OCPP messages to MQTT topics. New snoop clients could be implemented to
forward messages to other protocols or systems without modifying the relay. More than
one snoop client can connect to the snoop port at the same time and all will receive
a copy of all messages.

The relay supports multiple charge points connecting at the same time, though all
charge points must use the same CPMS URL. Snoop clients will receive messages from
all charge points. The JSON stream passed to snoop clients includes charge point IDs.
"""

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Relay OCPP traffic between a charge point and a CPMS.",
        epilog=msg)

    parser.add_argument('--config', type=str, default=None,
        help="""Path to YAML config file. Values in a 'relay' section will be used as defaults.""")

    parser.add_argument('--cpms', type=str, default=None,
        help="""URL of the real CPMS.""")

    parser.add_argument('--ocpp-host', type=str, default=None,
        help="""OCPP relay server interface address (default: all interfaces).""")
    parser.add_argument('--ocpp-port', type=int, default=8500,
        help="""OCPP relay server port for charge point connections (default: %(default)d).""")
    parser.add_argument('--snoop-host', type=str, default='localhost',
        help="""Snoop server interface address (default: %(default)s).""")
    parser.add_argument('--snoop-port', type=int, default=8501,
        help="""Snoop server port for clients that monitor OCPP traffic (default: %(default)d).""")

    parser.add_argument('--ssl-cert', default=None,
        help="""Path to SSL certificate file (default: None). Some chargers don't store
            trust chains and require that certificates are loaded onto the charger explicitly.""")
    parser.add_argument('--ssl-key', default=None,
        help="""Path to SSL private key file (default: None).""")

    # Verbose/quiet
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true', help="""Verbose output.""")
    group.add_argument('-q', '--quiet', action='store_true', help="""Reduce output.""")

    # First parse only the --config argument to allow YAML defaults
    preliminary = parser.parse_known_args()[0]

    yaml_defaults = {}
    if preliminary and preliminary.config:
        try:
            with open(preliminary.config, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
                yaml_defaults = cfg.get('relay', {}) if isinstance(cfg, dict) else {}
        except FileNotFoundError:
            print(f"Config file not found: {preliminary.config}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error loading config file {preliminary.config}: {e}", file=sys.stderr)
            sys.exit(1)

    # Apply YAML defaults where CLI didn't explicitly set a value
    for key, val in yaml_defaults.items():
        argname = f"--{key.replace('_', '-') }"
        # Only set default if not explicitly provided on the command line
        if not any(argname in a for a in sys.argv[1:]):
            dest = key
            if hasattr(parser, 'get_default'):
                parser.set_defaults(**{dest: val})

    global args
    args = parser.parse_args()

    if not args.cpms:
        print("CPMS URL must be set, either in a YAML config file or with --cpms.", file=sys.stderr)
        sys.exit(1)


def get_ssl_context(ssl_cert, ssl_key):
    """Return an SSL context if both ssl_cert and ssl_key are provided, else None."""
    if not ssl_cert or not ssl_key:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
    return ssl_context


async def core():
    ssl_context = get_ssl_context(args.ssl_cert, args.ssl_key)

    # Stream of messages passed by the OCPP relay. The stream is used to monitor
    # data and forward it to clients on the snoop port.
    msg_queue = asyncio.Queue()

    relay = OCPPRelay(args.cpms, snoop_queue=msg_queue)
    relay_server = await relay.start(args.ocpp_host, args.ocpp_port, ssl_context=ssl_context)

    # Snoop server to allow clients to connect and receive a copy of all messages
    # exchanged between charge points and the CSMS. Don't use SSL for localhost.
    snoop = SnoopWebSocketServer(snoop_queue=msg_queue)
    snoop_server = await snoop.start(args.snoop_host, args.snoop_port,
        ssl_context=(None if args.snoop_host == 'localhost' else ssl_context))

    await asyncio.gather(relay_server.wait_closed(), snoop_server.wait_closed())

def main():
    parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO),
        format='%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s')

    try:
        asyncio.run(core())
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == "__main__":
    main()