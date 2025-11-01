# -*- coding: utf-8 -*-

##
## WebSocket relay from charge points to a CSMS.
##

import asyncio
import base64
import json
import logging
import websockets

from ocpp2mqtt.common.types import MessageData


def basic_auth_header(username, password):
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return ("Authorization", f"Basic {basic_credentials}")


class OCPPRelay:
    """WebSocket relay that accepts connections from charge points and relays
    messages to/from a CSMS. The relay can optionally pass a copy of all
    messages to a snoop queue for monitoring. The combination of blindly
    relaying messages and passing a copy to a snoop queue makes it possible to
    map OCPP messages to MQTT topics without having to reimplement the OCPP."""

    def __init__(self, csms_url, csms_id=None, csms_pass=None, snoop_queue=None):
        if csms_url is None:
            raise ValueError("csms_url must not be None")
        self.logger = logging.getLogger()
        self.csms_url, self.csms_id, self.csms_pass = csms_url, csms_id, csms_pass
        self.snoop_queue = snoop_queue

    async def _relay(self, source_ws, target_ws, source_name, target_name, cp_id, protocol):
        while True:
            try:
                message = await source_ws.recv()
                json_message = json.loads(message)
                await target_ws.send(message)
                self.logger.info(f"Relayed message from {source_name} to {target_name} ({json_message[1]})")

                # Pass the message to the snoop queue
                if self.snoop_queue:
                    msg_data = MessageData(event="Message", sender=source_name, protocol=protocol, cp_id=cp_id, payload=json_message)
                    self.snoop_queue.put_nowait(msg_data)
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
                self.logger.info(f"{source_name} connection closed.")
                break

    async def _on_connect(self, ws):
        self.logger.info(f"WebSocket OnConnect for path {ws.request.path} on {ws.local_address}")
        self.logger.info(f"WebSocket request headers:\n{json.dumps(dict(ws.request.headers), indent=2)}")

        charge_point_id = ws.request.path.strip("/")
        cp_ws = ws
        try:
            ws_subprotocol = cp_ws.request.headers["Sec-WebSocket-Protocol"]
        except KeyError:
            self.logger.error(
                "Client didn't specify any sub-protocol. A sub-protocol is required for OCPP. Closing Connection."
            )
            return await cp_ws.close()

        self.logger.info(
            f"Received a new connection from a ChargePoint ID {charge_point_id}, protocol: {ws_subprotocol}"
        )
        self.logger.info(f"Connecting to CSMS at {self.csms_url}/{charge_point_id}")

        extra_headers = []
        if all([self.csms_id, self.csms_pass]):
            extra_headers.append(basic_auth_header(self.csms_id, self.csms_pass))

        csms_uri = f"{self.csms_url}/{charge_point_id}"

        # Use the async iterator form of websockets.connect which yields a
        # connected websocket each time a connection is established. When the
        # connection closes, the iterator continues and will attempt to
        # reconnect. We must stop attempting reconnects if the ChargePoint
        # (client) websocket closes.
        async for csms_ws in websockets.connect(
            csms_uri,
            subprotocols=[ws_subprotocol],
            additional_headers=extra_headers,
        ):
            # If the client side disconnected while we were trying to connect,
            # there's no point in reconnecting to the CSMS.
            if getattr(cp_ws, "closed", False):
                self.logger.info("ChargePoint websocket closed before CSMS connection; aborting reconnect attempts.")
                break

            try:
                await asyncio.gather(
                    self._relay(cp_ws, csms_ws, source_name="CP", target_name="CSMS", cp_id=charge_point_id, protocol=ws_subprotocol),
                    self._relay(csms_ws, cp_ws, source_name="CSMS", target_name="CP", cp_id=charge_point_id, protocol=ws_subprotocol),
                )
            except asyncio.CancelledError:
                # Propagate cancellations
                raise
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK) as e:
                self.logger.info(f"CSMS connection closed: {e}; async-for will attempt reconnect if appropriate")
                # Loop will continue and websockets.connect will try to reconnect
            except Exception as e:
                # Log unexpected errors and let the async-for attempt to reconnect
                self.logger.warning(f"Error during relaying to CSMS: {e}; will attempt to reconnect")

            # If the ChargePoint disconnected while we were relaying, stop retrying
            if getattr(cp_ws, "closed", False):
                self.logger.info("ChargePoint websocket closed; stopping CSMS reconnect attempts.")
                break

    async def start(self, host, port, ssl_context=None):
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self.logger.info(f"Relay server started on {port}")
        return server
