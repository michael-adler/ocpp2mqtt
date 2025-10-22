# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, Literal
from dataclasses_json import dataclass_json, DataClassJsonMixin


@dataclass_json
@dataclass
class MessageData(DataClassJsonMixin):
    """Data class for messages passed to the snoop queue."""
    event: Literal["Connection", "Disconnection", "Message"]
    sender: Literal["CP", "CSMS"]
    protocol: str = None
    cp_id: str = None
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

@dataclass_json
@dataclass
class MQTTData():
    """Data class for messages passed to the MQTT queue. For Home Assistant the discovery
    topic will be homeassistant/device/ocpp/<unique_id>/config and the state will be
    published to ocpp/<cp_id>/<topic>/state as value_json.<value_type>."""
    device_class: str = "sensor"
    cp_id: str = None
    topic: str = None
    unique_id: str = None
    vendor_id: str = None
    value: Any = None
    value_type: str = None
    unit: str = None
    timestamp: str = None
