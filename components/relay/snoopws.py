# -*- coding: utf-8 -*-

##
## WebSocket server that forwards OCPP messages from snoop_queue to any client
## that connects to the server. Multiple clients can connect and each receives
## the same messages.
##
## A client could receive the JSON stream, look for messages about energy
## usage and forward messages to an MQTT broker.
##

import asyncio
from dataclasses import asdict
import json
import logging
import websockets

class SnoopWebSocketServer:
    """WebSocket server that forwards OCPP messages from snoop_queue to all
    connected snoop clients.
    """
    def __init__(self, snoop_queue):
        if snoop_queue is None:
            raise ValueError("snoop_queue must be set")

        self.logger = logging.getLogger()
        self.snoop_queue = snoop_queue

        # Set of currently connected snoop clients
        self.snoop_sockets = set()

        asyncio.create_task(self._forward_messages())

    async def _forward_messages(self):
        """The main worker task that consumes messages from the snoop queue and
        forwards them to the set of connected snoop clients. The same message
        is sent to all connected clients."""
        while True:
            msg = await self.snoop_queue.get()
            self.logger.debug(f"Message from queue:\n{json.dumps(asdict(msg), indent=2)}")

            msg_json = json.dumps(asdict(msg))
            for ws in self.snoop_sockets.copy():
                try:
                    await ws.send(msg_json)
                except Exception as e:
                    self.logger.error(f"Error sending message to snoop client: {e}")
                    self.snoop_sockets.remove(ws)

    async def _relay(self, source_ws):
        while True:
            try:
                message = await source_ws.recv()
                self.logger.debug(f"Ignored message: ({message})")
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
                self.logger.info(f"Snoop connection closed.")
                break

    async def _on_connect(self, ws):
        self.logger.info(f"Received a new connection from a snoop client.")
        self.snoop_sockets.add(ws)
        await self._relay(ws)
        self.snoop_sockets.remove(ws)

    async def start(self, host, port, ssl_context=None):
        """Start the snoop WebSocket server on the given address and port."""
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self.logger.info(f"Snoop server started on {port}")
        return server
