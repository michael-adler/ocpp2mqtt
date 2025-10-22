# -*- coding: utf-8 -*-

import logging

from ocpp2mqtt.common.types import MessageData, MQTTData


class OCPPFilter:
    """
    Stateful filter for OCPP messages. Returns lists of data that will
    be sent to MQTT."""
    def __init__(self):
        self._logger = logging.getLogger()
        # Will be indexed by cp_id
        self._vendor_id = {}

    def filter(self, msg: MessageData) -> list | None:
        if msg.event != "Message": return None
        if msg.sender != "CP": return None

        cp_id = msg.cp_id
        protocol = msg.protocol
        if protocol and protocol.lower().startswith("ocpp"):
            protocol = protocol[4:]
        
        # The OCPP message itself
        ocpp = msg.payload

        # All the messages of interest have 4 top-level elements
        if not isinstance(ocpp, list) or len(ocpp) < 4:
            return None
        # Look only for requests
        if ocpp[0] != 2:
            return None

        if cp_id not in self._vendor_id:
            self._vendor_id[cp_id] = None
        if not self._vendor_id[cp_id]:
            self._vendor_id[cp_id] = self._get_vendor_id(ocpp)

        if protocol == "1.6":
            return self._filter_ocpp16(cp_id, msg.timestamp, ocpp)
        else:
            return self._filter_ocpp20(cp_id, msg.timestamp, ocpp)

    def _get_vendor_id(self, ocpp: list) -> str | None:
        """
        Extract vendor ID from OCPP message if available.
        """
        action = ocpp[2]
        payload = ocpp[3]

        if action == "DataTransfer":
            if isinstance(payload, dict) and 'vendorId' in payload:
                return payload.get('vendorId')

        return None

    def _new_MQTTData(self, cp_id: str, timestamp: str) -> MQTTData:
        m = MQTTData()
        m.cp_id = cp_id
        m.vendor_id = self._vendor_id[cp_id]
        m.timestamp = timestamp
        return m

    def _filter_ocpp16(self, cp_id: str, timestamp: str, ocpp: list) -> list | None:
        """
        Filter OCPP 1.6 messages.
        """
        action = ocpp[2]
        payload = ocpp[3]

        if action == "StatusNotification":
            self._logger.debug(f"OCPP 1.6 StatusNotification from {cp_id}: {payload}")
            m = self._new_MQTTData(cp_id, timestamp)
            m.topic = f"{payload.get('connectorId')}/status"
            m.unique_id = f"OCPP_{cp_id}_{payload.get('connectorId')}_status"
            m.value = payload.get('status')
            return [m]
        elif action == "MeterValues":
            self._logger.debug(f"OCPP 1.6 MeterValues from {cp_id}: {payload}")
            messages = []
            for mv in payload.get('meterValue', []):
                for v in mv.get('sampledValue', []):
                    m = self._new_MQTTData(cp_id, timestamp)
                    value_type = v.get('measurand').replace(".", "-")
                    # Only process energy measurements for now
                    if not isinstance(value_type, str) or not value_type.startswith("Energy"):
                        continue

                    topic = payload.get('connectorId')
                    if v.get('location'):
                        topic = f"{topic}/{v.get('location')}"
                    else:
                        topic = f"{topic}/Outlet"
                    m.topic = f"{topic}/{value_type}"
                    m.unique_id = f"OCPP_{cp_id}_{topic.replace('/', '_')}"
                    m.value = v.get('value')
                    m.value_type = 'energy'
                    m.unit = v.get('unit')
                    messages.append(m)
            return messages

        return None
    
    def _filter_ocpp20(self, cp_id: str, timestamp: str, ocpp: list) -> list | None:
        """
        Filter OCPP 2.0 messages.

        *** This filter is untested and needs to be verified with a charge point using OCPP 2.0 ***
        """
        action = ocpp[2]
        payload = ocpp[3]

        if action == "StatusNotification":
            self._logger.debug(f"OCPP 2.0 StatusNotification from {cp_id}: {payload}")
            m = self._new_MQTTData(cp_id, timestamp)
            # Use evseId for OCPP 2.0. The connectorId indicates a cable within the evseId
            # but only one cable can be active at a time. The MeterValues are per evseId, so
            # record status globally for the evseId.
            m.topic = f"{payload.get('evseId')}/status"
            m.unique_id = f"OCPP_{cp_id}_{payload.get('evseId')}_status"
            m.value = payload.get('connectorStatus')
            return [m]
        elif action == "MeterValues":
            self._logger.debug(f"OCPP 2.0 MeterValues from {cp_id}: {payload}")
            messages = []
            for mv in payload.get('meterValue', []):
                for v in mv.get('sampledValue', []):
                    m = self._new_MQTTData(cp_id, timestamp)
                    value_type = v.get('measurand')
                    if not isinstance(value_type, str):
                        value_type = "Energy.Active.Import.Register"
                    value_type = value_type.replace(".", "-")
                    # Only process energy measurements for now
                    if not value_type.startswith("Energy"):
                        continue

                    topic = payload.get('evseId')
                    if v.get('location'):
                        topic = f"{topic}/{v.get('location')}"
                    else:
                        topic = f"{topic}/Outlet"
                    m.topic = f"{topic}/{value_type}"
                    m.unique_id = f"OCPP_{cp_id}_{topic.replace('/', '_')}"
                    m.value = v.get('value')
                    m.value_type = 'energy'

                    unit = v.get('unitOfMeasure')
                    if not unit:
                        m.unit = 'Wh'
                    else:
                        m.unit = unit.get('unit')

                    messages.append(m)
            return messages

        return None