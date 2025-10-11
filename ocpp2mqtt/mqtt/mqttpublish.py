# -*- coding: utf-8 -*-

import asyncio
import logging
import paho.mqtt.client as mqtt_client

from ocpp2mqtt.common.types import MQTTData

class MQTTPublisher:
    def __init__(self, broker_host: str, broker_port: int = 1883, topic_prefix: str = ""):
        self.logger = logging.getLogger(__name__)
        self.queue = asyncio.Queue()
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic_prefix = topic_prefix
        self.exit_task = False

        self.mqtt = mqtt_client.Client()
        self.mqtt.connect_timeout = 5.0
        self.mqtt.on_connect = self._mqtt_on_connect
        self.mqtt.on_connect_fail = self._mqtt_on_connect_fail
        self.broker_connection_failed = False

    async def publish_data(self, data: MQTTData):
        """Push an MQTTData instance onto the queue."""
        await self.queue.put(data)

    async def run(self):
        """Asyncio task: consume the queue and publish to MQTT broker."""
        while True:
            try:
                data: MQTTData = await asyncio.wait_for(self.queue.get(), timeout=5.0)
                topic = f"{self.topic_prefix}{data.id}"
                payload = str(data.value)
                self.logger.info(f"Publishing {data}")
                #self.mqtt.publish(topic, payload)
                self.queue.task_done()
            except asyncio.TimeoutError:
                # Timeout is a chance to check for connection failure below
                pass

            if self.exit_task and self.queue.empty():
                break
            if self.broker_connection_failed:
                self.logger.error("Broker connection failed, stopping publisher.")
                break

        self.mqtt.loop_stop()
        self.mqtt.disconnect()
        if self.broker_connection_failed:
            raise RuntimeError("Broker connection failed, stopped publisher.")
        self.logger.info("MQTTPublisher run() task exiting.")

    async def start(self):
        import sys
        self.logger.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}...")
        self.mqtt.loop_start()

        self.mqtt.connect_async(self.broker_host, self.broker_port)
        await self.run()

    async def stop(self):
        self.logger.info("Stopping MQTTPublisher...")
        self.mqtt.loop_stop()
        self.mqtt.disconnect()

        # self.queue.shutdown() would work well here, but it requires Python 3.11+
        self.exit_task = True

    def _mqtt_on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback function for when the client connects to the MQTT broker."""
        if rc == 0:
            self.logger.info(f"Connection successful to {self.broker_host}:{self.broker_port}...")
        else:
            self.logger.error(f"Error connecting to {self.broker_host}:{self.broker_port}, return code {rc}")
            self.broker_connection_failed = True

    def _mqtt_on_connect_fail(self, client, userdata):
        """Callback function for when the client fails to connect to the MQTT broker."""
        self.logger.error(f"Failed to connect to MQTT broker {self.broker_host}:{self.broker_port}")
        self.broker_connection_failed = True
