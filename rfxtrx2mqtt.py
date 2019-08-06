from collections import namedtuple
import argparse
import asyncio
import json
import logging
import signal

import RFXtrx
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from hbmqtt.errors import HBMQTTException


log = logging.getLogger("rfxtrx2mqtt")

rfxtrx2mqtt_version = "0.0.1"
rfxtrx2mqtt_base_topic = "rfxtrx2mqtt"

# Discovery topic format: <discovery_prefix>/<component>/[<node_id>/]<object_id>/config
homeassistant_discovery_topic_prefix = "homeassistant"

known_devices = {}

DEFAULT_CONFIG = {
    "rfxtrx": {
        "device": "/dev/tty.usbserial-A11Q57E2",
    },

    "mqtt": {
        "broker_host": "localhost",
        "broker_port": 1883,
        "client_id": None,

        "base_topic": "rfxtrx2mqtt",
    },

    "homeassistant": {
        "discovery_prefix": "homeassistant",
    },

    "whitelisted_id_strings": [
        ""
    ],
}


def get_discovery_topic(component, device_id, sensor_id):
    assert component == "sensor", "rfxtrx2mqtt only supports sensor components yet"
    # Sensor ID is the same as object_id in the HA mqtt discovery topic format.
    discovery_topic = f"{homeassistant_discovery_topic_prefix}/{component}/{device_id}/{sensor_id}/config"
    return discovery_topic


def get_state_topic(component, device_id):
    assert component == "sensor", "rfxtrx2mqtt only supports sensor components yet"
    state_topic = f"{rfxtrx2mqtt_base_topic}/{component}/{device_id}/state"
    return state_topic


# Devices from pyRFXtrx have an id_string attribute, but the
# __eq__ method also compares subtype and packettype in addition
# to id_string. This makes sense, as a device will not change its
# packettype and subtype. Some of the device types already include
# for example packettype in the id_string, but not all of them do
# that (for reasons I don't understand). So here I consider the
# tuple of (packettype, subtype, id_string) to be the unique
# device identifer.
#
# The returned string converts packettype and subtype to hex
# string (similar to how id_string looks like for many device
# types).
#
# See also https://github.com/Danielhiversen/pyRFXtrx/blob/master/RFXtrx/__init__.py
#
# In Home Assistant the rfxtrx module can automatically add
# sensors and generate IDs from them (note: sensors, not devices!
# A (physical) device can have multiple sensors (in the HA world,
# such as both temp and humidity). The sensor ID that HA generates
# uses an entire packet from the device, including id, packettype,
# subtype, but also packet counter, temperature reading (in the
# case of a temp sensor), and what other data that might be
# sent. This makes no sense to me, it seems very strange to
# include the sensor values in the ID. So I won't replicate that
# behavior in rfxtrx2mqtt.
def create_device_id(rfxtrx_device):
    packettype = f"{rfxtrx_device.packettype:02x}"
    subtype = f"{rfxtrx_device.subtype:02x}"
    id_string = rfxtrx_device.id_string.replace(":", "")
    return f"device_{packettype}_{subtype}_{id_string}"


class Device:
    def __init__(self, *, device_id, rfxtrx_device, model, sensors):
        self.device_id = device_id
        self.rfxtrx_device = rfxtrx_device
        self.model = model
        self.sensors = sensors


class Sensor:
    def __init__(self, *, sensor_id, sensor_type, event_value_key):
        # The sensor id is relative to the device
        self.sensor_id = sensor_id

        # https://developers.home-assistant.io/docs/en/entity_sensor.html
        # sensor_type = device_class
        assert sensor_type in ("temperature", "humidity")
        self.sensor_type = sensor_type
        if sensor_type == "temperature":
            self.unit_of_measurement = "°C"
        elif sensor_type == "humidity":
            self.unit_of_measurement = "%"


def create_device_config(device):
    device_config = {
        "name": f"rfxtrx_{device.rfxtrx_device.id_string.replace(':', '')}",
        "identifiers": [f"rfxtrx2mqtt_{device.device_id}"],
        "sw_version": f"rfxtrx2mqtt {rfxtrx2mqtt_version}",
        "model": f"{device.model}",
        "manufacturer": f"rfxtrx2mqtt",
    }
    return device_config


def create_sensor_config(device, sensor):
    component = "sensor"
    config = {
        "name": f"rfxtrx_{device.rfxtrx_device.id_string.replace(':', '')}_{sensor.sensor_id}",
        "device_class": f"{sensor.sensor_type}",
        "unit_of_measurement": f"{sensor.unit_of_measurement}",
        "state_topic": get_state_topic(component, device.device_id),
        "value_template": f"{{{{ value_json.{sensor.sensor_id} }}}}",
        "unique_id": f"rfxtrx2mqtt_{device.device_id}_{sensor.sensor_id}",
        "device": create_device_config(device),
    }
    return config


event_value_key_to_sensor_id_map = {
    "Temperature": "temperature",
    "Temperature2": "temperature_2",
    "Humidity": "humidity",
}

event_value_key_to_sensor_type_map = {
    "Temperature": "temperature",
    "Temperature2": "temperature",
    "Humidity": "humidity",
}


def get_sensors(event):
    sensors = {}
    for key in event.values:
        if key not in event_value_key_to_sensor_id_map:
            continue
        sensor_id = event_value_key_to_sensor_id_map[key]
        sensors[sensor_id] = Sensor(
            sensor_id=sensor_id,
            sensor_type=event_value_key_to_sensor_type_map[key],
            event_value_key=key)
    return sensors


def create_device(event):
    device_id = create_device_id(event.device)
    sensors = get_sensors(event)
    device = Device(
        device_id=device_id,
        rfxtrx_device=event.device,
        model=event.device.type_string,
        sensors=sensors)
    return device


async def send_discovery(client, device):
    component = "sensor"
    for sensor_id, sensor in device.sensors.items():
        topic = get_discovery_topic(component, device.device_id, sensor_id)
        config = create_sensor_config(device, sensor)
        log.debug(f"Publishing discovery config '{config}' for device '{device.device_id}' on topic '{topic}'")
        msg = json.dumps(config)
        # todo: add retain flag (zigbee2mqtt does that, with qos=0)
        await client.publish(topic, msg.encode("utf-8"))


def event_values_to_state(values):
    state = {}
    for key in values:
        if key not in event_value_key_to_sensor_id_map:
            continue
        sensor_id = event_value_key_to_sensor_id_map[key]
        state[sensor_id] = values[key]
    return state


async def send_state(client, device_id, event):
    component = "sensor"
    topic = get_state_topic(component, device_id)
    state = event_values_to_state(event.values)
    log.debug(f"Publishing state '{state}' for device_id '{device_id}' on topic '{topic}'")
    msg = json.dumps(state)
    await client.publish(topic, msg.encode("utf-8"))


async def handle_event(event, mqtt_client):
    try:
        log.debug(f"Got event {event.__dict__}) from device {event.device.__dict__}")
        if not isinstance(event, RFXtrx.SensorEvent):
            log.info(f"Ignoring event, not a sensor event! Event: {event}")
            return

        device = create_device(event)
        log.info(f"Device with ID '{device.device_id}' ({device.model}) sent sensor values: {event.values}")

        if device.device_id not in known_devices:
            log.info(f"Found new device: id_string: '{device.rfxtrx_device.id_string}', type_string: '{device.rfxtrx_device.type_string}'), device ID: {device.device_id}")
            known_devices[device.device_id] = device
            await send_discovery(mqtt_client, device)

        state = event_values_to_state(event.values)
        await send_state(mqtt_client, device.device_id, event)
    except Exception:
        log.exception("Exception in handle_event")


async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info(f"Flushing metrics")
    loop.stop()


def setup_rfxtrx(loop, mqtt_client, debug):
    rfxtrx_device = "/dev/tty.usbserial-A11Q57E2"

    def rfxtrx_event_callback(event):
        asyncio.run_coroutine_threadsafe(handle_event(event, mqtt_client), loop)

    log.info(f"Using RFXtrx device '{rfxtrx_device}'")
    rfxtrx_conn = RFXtrx.Connect(rfxtrx_device, rfxtrx_event_callback, debug=debug)
    return rfxtrx_conn


def shutdown_rfxtrx(rfxtrx_conn):
    log.info("Shutting down RFXtrx")
    rfxtrx_conn.close_connection()


async def run(args):
    loop = asyncio.get_running_loop()
    try:
        mqtt_client = MQTTClient(client_id="rfxtrx2mqtt")
        ret = await mqtt_client.connect("mqtt://localhost:1883/", cleansession=True)
    except ConnectException as ce:
        log.error("Connection failed: %s" % ce)
        # todo: do what?
        return

    rfxtrx_conn = await loop.run_in_executor(
        None, setup_rfxtrx, loop, mqtt_client, args.debug)

    await asyncio.sleep(1.0)

    try:
        while True:
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass

    await loop.run_in_executor(None, shutdown_rfxtrx, rfxtrx_conn)
    await mqtt_client.disconnect()


def read_config():
    # TODO: Read config file
    return DEFAULT_CONFIG


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--config", help="Config file")
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    formatter = "%(asctime)s %(levelname)-7s [%(name)-20s] %(message)s"
    logging.basicConfig(level=loglevel, format=formatter)

    log.info("Starting rfxtrx2mqtt")
    asyncio.run(run(args))
    log.info("Exiting rfxtrx2mqtt")


if __name__ == "__main__":
    main()
