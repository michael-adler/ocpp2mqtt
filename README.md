# OCPP Relay with Snooping and MQTT Metering

EV chargers are typically controlled by the [Open Charge Point Protocol](https://openchargealliance.org/protocols/open-charge-point-protocol/) \(OCPP\). The protocol includes metering. Extracting metering data from a charger is challenging if you have to implement a full Charge Point Management System \(CPMS\). The relay acts as a full CPMS to an EV charger but implements the service by relaying all requests to a real CPMS. The relay exposes a second service where OCPP traffic can be monitored. No commands are accepted on this snoop port. Multiple clients may connect to the snoop service and each one receives the same OCPP JSON stream.

The relay [ocpp-relay-server.py](ocpp-relay-server.py) should work with any charge point. Data is forwarded blindly. It is derived from https://github.com/saisasidhar/ocpp-relay.

The provided MQTT snoop client matches OCPP message patterns and maps them to MQTT. Some metering data is standardized by OCPP and some varies by charge point.

By default the relay runs on port 8500 and the snoop service on port 8501. Configure an EV charger to connect to ws://your-server:8500/ and set the relay's ocpp-host to the server in the EV charger. The relay does support SSL, though some chargers don't have root certificates and require uploading all or part of your certificate chain.
