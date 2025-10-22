#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import logging
import sys

from ocpp2mqtt.mqtt.mqttpublish import MQTTPublisher
from ocpp2mqtt.mqtt.ocppsnoop import receive_ocpp_snoop, receive_ocpp_from_file
from ocpp2mqtt.mqtt.ocppfilter import OCPPFilter

def parse_args():
    """Parse command line arguments."""

    msg = """
Map a stream of OCPP messages to MQTT topics.
"""

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Map OCPP metering data from a charge point to MQTT topics.",
        epilog=msg)

    parser.add_argument('--snoop-socket', type=str, default='ws://localhost:8501/',
        help="""URL of the OCPP relay's snoop port (default: %(default)s).""")

    parser.add_argument('--mqtt-broker-host', type=str, default='localhost',
        help="""Hostname or IP address of the MQTT broker (default: %(default)s).""")
    parser.add_argument('--mqtt-broker-port', type=int, default=1883,
        help="""Port of the MQTT broker (default: %(default)s).""")
    parser.add_argument('--mqtt-broker-username', type=str, default=None,
        help="""Username for MQTT broker authentication (default: %(default)s).""")
    parser.add_argument('--mqtt-broker-password', type=str, default=None,
        help="""Password for MQTT broker authentication (default: %(default)s).""")

    # Verbose/quiet
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true', help="""Verbose output.""")
    group.add_argument('-q', '--quiet', action='store_true', help="""Reduce output.""")

    global args
    args = parser.parse_args()


async def process_messages(publisher):
    logger = logging.getLogger()
    ocpp_filter = OCPPFilter()

    async for msg in receive_ocpp_snoop(ws_uri=args.snoop_socket):
    #for msg in receive_ocpp_from_file("../ocpp2mqtt.orig/output.json"):
        filtered = ocpp_filter.filter(msg)
        if filtered:
            for m in filtered:
                logger.info(f"Handle message: {m}")
                await publisher.publish_data(m)

    logger.info("Message source closed. Stopping publisher...")
    publisher.stop()


async def core():
    global args
    logger = logging.getLogger()

    # Instantiate the MQTT publisher
    publisher = MQTTPublisher(broker_host=args.mqtt_broker_host,
                              broker_port=args.mqtt_broker_port,
                              broker_username=args.mqtt_broker_username,
                              broker_password=args.mqtt_broker_password)

    # Run both publisher and message processing concurrently
    await asyncio.gather(
        publisher.start(),
        process_messages(publisher)
        )

def main():
    parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO),
        format='%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s')

    try:
        asyncio.run(core())
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(1)

if __name__ == "__main__":
    main()
