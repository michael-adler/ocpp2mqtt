# -*- coding: utf-8 -*-

import websockets
import logging
import json

from ocpp2mqtt.common.types import MessageData


async def receive_ocpp_snoop(ws_uri: str):
    """
    Async generator that yields MessageData objects from a websocket until closed.
    Usage:
        async for msg in receive_ocpp_snoop(ws_uri):
            ...
    """
    logger = logging.getLogger()
    logger.info(f"Connecting to {ws_uri}...")
    async for websocket in websockets.connect(ws_uri):
        try:
            async for message in websocket:
                try:
                    msg = MessageData(**json.loads(message))
                    yield msg
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}\nMessage: {message}")
        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"Connection closed: {e.code} ({e.reason}). Retrying...")
            continue


def receive_ocpp_from_file(file_path: str):
    """
    Generator that yields MessageData objects from a file until EOF.
    Usage:
        for msg in receive_ocpp_from_file(file_path):
            ...
    """
    logger = logging.getLogger()
    logger.info(f"Reading OCPP messages from {file_path}...")
    try:
        with open(file_path, "r", encoding="utf-8") as infile:
            for line in infile:
                try:
                    msg = MessageData(**json.loads(line))
                    yield msg
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON: {e}\nLine: {line}")
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
