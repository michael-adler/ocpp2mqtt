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
        self._manufacturer = {}

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

        if cp_id not in self._manufacturer:
            self._manufacturer[cp_id] = None
        if not self._manufacturer[cp_id]:
            self._manufacturer[cp_id] = self._get_manufacturer(ocpp)

        if ocpp[2] == "Heartbeat":
            m = self._new_MQTTData(cp_id, msg.timestamp)
            m.topic = "heartbeat"
            m.unique_id = f"OCPP_{cp_id}_heartbeat"
            m.name = f"Heartbeat CP {cp_id}"
            m.device_class = "timestamp"
            m.value = msg.timestamp
            return [m]

        if protocol == "1.6":
            return self._filter_ocpp16(cp_id, msg.timestamp, ocpp)
        else:
            return self._filter_ocpp20(cp_id, msg.timestamp, ocpp)

    def _get_manufacturer(self, ocpp: list) -> str | None:
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
        m.manufacturer = self._manufacturer[cp_id]
        m.timestamp = timestamp
        return m

    def _new_meter_MQTTData(self, cp_id: str, timestamp: str, value_type: str,
                            evse_id: str, location: str,
                            value: str, unit: str) -> MQTTData:
        """
        Create a new MQTTData instance for a meter value. This code is common to OCPP 1.6
        and OCPP 2.0.
        """
        if not isinstance(value_type, str):
            value_type = "Energy.Active.Import.Register"
        value_type = value_type.replace(".", "-")

        if not isinstance(value_type, str):
            return None

        if value_type.startswith("Current"):
            sensor_type = 'current'
        elif value_type.startswith("Energy"):
            sensor_type = 'energy'
        elif value_type.startswith("Power"):
            sensor_type = 'power'
        elif value_type.startswith("Voltage"):
            sensor_type = 'voltage'
        else:
            return None

        if not location:
            location = "Outlet"

        m = self._new_MQTTData(cp_id, timestamp)
        m.topic = f"{evse_id}/{location}/{value_type}"
        m.unique_id = f"OCPP_{cp_id}_{m.topic}".replace('/', '_')

        if not evse_id:
            m.name = f"{value_type.replace('-', ' ')} {location} CP {cp_id}"
        else:
            m.name = f"C{evse_id} {value_type.replace('-', ' ')} {location} CP {cp_id}"

        m.value = value
        m.device_class = sensor_type
        m.unit = unit

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
            cable_id = payload.get('connectorId')
            m.topic = f"{cable_id}/status"
            m.unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            if not cable_id:
                m.name = f"Status CP {cp_id}"
            else:
                m.name = f"C{cable_id} Status CP {cp_id}"
            m.value = payload.get('status')
            return [m]
        elif action == "MeterValues":
            self._logger.debug(f"OCPP 1.6 MeterValues from {cp_id}: {payload}")
            messages = []
            for mv in payload.get('meterValue', []):
                for v in mv.get('sampledValue', []):
                    m = self._new_meter_MQTTData(cp_id, timestamp, v.get('measurand'),
                                                 payload.get('connectorId'),
                                                 v.get('location'),
                                                 v.get('value'), v.get('unit'))
                    if not m:
                        continue

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
            cable_id = payload.get('evseId')
            m.topic = f"{cable_id}/status"
            m.unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            if not cable_id:
                m.name = f"Status CP {cp_id}"
            else:
                m.name = f"C{cable_id} Status CP {cp_id}"
            m.value = payload.get('connectorStatus')
            return [m]
        elif action == "MeterValues":
            self._logger.debug(f"OCPP 2.0 MeterValues from {cp_id}: {payload}")
            messages = []
            for mv in payload.get('meterValue', []):
                for v in mv.get('sampledValue', []):
                    unit = v.get('unitOfMeasure')
                    if not unit:
                        unit = 'Wh'
                    else:
                        unit = unit.get('unit')

                    m = self._new_meter_MQTTData(cp_id, timestamp, v.get('measurand'),
                                                 payload.get('evseId'),
                                                 v.get('location'),
                                                 v.get('value'), unit)
                    if not m:
                        continue

                    messages.append(m)
            return messages

        return None