import argparse
import asyncio
import collections
import collections.abc
import copy
import datetime
import json
import logging
import signal
import sys
import time
from pathlib import Path

import RFXtrx
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from hbmqtt.errors import HBMQTTException
import yaml


log = logging.getLogger("rfxtrx2mqtt")

rfxtrx2mqtt_version = "0.0.1"

seen_devices = {}
seen_devices_last_values = {}
seen_devices_last_timestamp = {}

DEFAULT_CONFIG = {
    "rfxtrx": {
        "device": None,
    },

    "mqtt": {
        "broker_host": "localhost",
        "broker_port": 1883,
        "client_id": "rfxtrx2mqtt",

        "base_topic": "rfxtrx2mqtt",
    },

    "homeassistant": {
        # Discovery topic format: <discovery_prefix>/<component>/[<node_id>/]<object_id>/config
        "discovery_prefix": "homeassistant",
    },

    "whitelist": [
        # Example:
        #
        #   { "device_id": "5209_9700" }
    ],

    "device_name_map": {
    },

    "sensor_name_map": {
        # Example:
        #
        #   "5002_6501": "outdoor_north",
    }
}


event_value_key_to_sensor_id_map = {
    "Temperature": "temperature",
    "Temperature2": "temperature_2",
    "Humidity": "humidity",
    "Battery numeric": "battery",
    "Rssi numeric": "rssi",
}

event_value_key_to_sensor_type_map = {
    "Temperature": "temperature",
    "Temperature2": "temperature",
    "Humidity": "humidity",
    "Battery numeric": "battery_numeric",
    "Rssi numeric": "rssi_numeric",
}


def get_discovery_topic(component, device_id, sensor_id, config):
    assert component == "sensor", "rfxtrx2mqtt only supports sensor components yet"
    # Sensor ID is the same as object_id in the HA mqtt discovery topic format.
    discovery_topic = f"{config['homeassistant']['discovery_prefix']}/{component}/{device_id}/{sensor_id}/config"
    return discovery_topic


def get_state_topic(component, device_id, config):
    assert component == "sensor", "rfxtrx2mqtt only supports sensor components yet"
    state_topic = f"{config['mqtt']['base_topic']}/{component}/{device_id}/state"
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
    return f"{packettype}{subtype}_{id_string}"


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
        # device_class is derived from sensor_type, but not all
        # sensor_type:s are valid device_classes (HA does not have a
        # "battery numeric" or "rssi numeric" device class.
        assert sensor_type in event_value_key_to_sensor_type_map.values()

        self.sensor_type = sensor_type
        if sensor_type == "temperature":
            self.unit_of_measurement = "°C"
        elif sensor_type == "humidity":
            self.unit_of_measurement = "%"
        elif sensor_type == "battery_numeric":
            self.unit_of_measurement = None
        elif sensor_type == "rssi_numeric":
            self.unit_of_measurement = None


def get_device_name(device, config):
    return config["device_name_map"].get(device.device_id, f"rfxtrx_{device.device_id}")


def get_sensor_name(device, sensor, config):
    device_prefix = config["sensor_name_map"].get(device.device_id, f"rfxtrx_{device.device_id}")
    return f"{device_prefix}_{sensor.sensor_id}"


def create_device_config(device, config):
    device_config = {
        "name": get_device_name(device, config),
        "identifiers": [f"rfxtrx2mqtt", f"{device.device_id}"],
        "sw_version": f"rfxtrx2mqtt {rfxtrx2mqtt_version}",
        "model": f"{device.model}",
        "manufacturer": f"rfxtrx2mqtt",
    }
    return device_config


def create_sensor_config(device, sensor, config):
    component = "sensor"
    sensor_config = {
        "name": get_sensor_name(device, sensor, config),
        "state_topic": get_state_topic(component, device.device_id, config),
        "value_template": f"{{{{ value_json.{sensor.sensor_id} }}}}",
        "unique_id": f"rfxtrx2mqtt_{device.device_id}_{sensor.sensor_id}",
        "device": create_device_config(device, config),
    }

    if sensor.unit_of_measurement:
        sensor_config["unit_of_measurement"] = f"{sensor.unit_of_measurement}"

    # Only temperature and humidity have correct device classes in Home Assistant.
    if sensor.sensor_type in ("temperature", "humidity"):
        sensor_config["device_class"] = f"{sensor.sensor_type}"

    return sensor_config


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


async def publish_discovery(client, device, config):
    component = "sensor"
    for sensor_id, sensor in device.sensors.items():
        topic = get_discovery_topic(component, device.device_id, sensor_id, config)
        sensor_config = create_sensor_config(device, sensor, config)
        log.debug(f"Publishing discovery config '{sensor_config}' for device '{device.device_id}' on topic '{topic}'")
        msg = json.dumps(sensor_config)
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


async def publish_state(client, device_id, event):
    component = "sensor"
    topic = get_state_topic(component, device_id)
    state = event_values_to_state(event.values)
    log.debug(f"Publishing state '{state}' for device_id '{device_id}' on topic '{topic}'")
    msg = json.dumps(state)
    await client.publish(topic, msg.encode("utf-8"))


async def handle_event(event, mqtt_client, config):
    try:
        log.debug(f"Got event {event.__dict__}) from device {event.device.__dict__}")
        if not isinstance(event, RFXtrx.SensorEvent):
            log.info(f"Ignoring event, not a sensor event! Event: {event}")
            return

        device = create_device(event)

        whitelisted_device_ids = [ w["device_id"] for w in config["whitelist"] ]
        if not config["whitelist"]:
            is_whitelisted = True
        elif device.device_id in whitelisted_device_ids:
            is_whitelisted = True
        else:
            is_whitelisted = False

        log.info(f"Event: Device with ID '{device.device_id}' (whitelisted: {is_whitelisted}) sent sensor values: {event.values}")
        if device.device_id not in seen_devices:
            log.info(f"Found new device: Device ID: {device.device_id}, packettype '{device.rfxtrx_device.packettype:02x}', subtype: '{device.rfxtrx_device.subtype:02x}', id_string: '{device.rfxtrx_device.id_string}', type_string: '{device.rfxtrx_device.type_string}', whitelisted: {is_whitelisted}")
            seen_devices[device.device_id] = device
            if is_whitelisted:
                await publish_discovery(mqtt_client, device, config)
            else:
                log.debug(f"Device ID {device.device_id} not whitelisted, not publishing discovery config")

        seen_devices_last_values[device.device_id] = event.values
        seen_devices_last_timestamp[device.device_id] = time.time()

        # Whitelist is not enabled if it's empty
        if is_whitelisted:
            state = event_values_to_state(event.values)
            await publish_state(mqtt_client, device.device_id, event)
        else:
            log.debug(f"Device ID {device.device_id} not whitelisted, not publishing event state")
    except Exception:
        log.exception("Exception in handle_event")


def log_seen_devices(config):
    whitelisted_device_ids = [ w["device_id"] for w in config["whitelist"] ]

    log.info(f"Seen devices ({len(seen_devices)})")
    log.info("----------------------------------------")
    for device_id in sorted(seen_devices.keys()):
        device = seen_devices[device_id]
        if device.device_id in whitelisted_device_ids:
            is_whitelisted = True
        else:
            is_whitelisted = False
        log.info(f" * Device ID: {device.device_id}\n"
                 f"   packettype: {device.rfxtrx_device.packettype:02x}\n"
                 f"   subtype: {device.rfxtrx_device.subtype:02x}\n"
                 f"   id_string: {device.rfxtrx_device.id_string}\n"
                 f"   type_string: {device.rfxtrx_device.type_string}\n"
                 f"   last values: {seen_devices_last_values[device.device_id]}\n"
                 f"   whitelisted: {is_whitelisted}\n"
                 f"   last seen: {datetime.datetime.utcfromtimestamp(seen_devices_last_timestamp[device.device_id]).strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("----------------------------------------")


async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def setup_rfxtrx(config, loop, mqtt_client, debug):
    rfxtrx_device = config["rfxtrx"]["device"]

    def rfxtrx_event_callback(event):
        asyncio.run_coroutine_threadsafe(handle_event(event, mqtt_client, config), loop)

    log.info(f"Using RFXtrx device '{rfxtrx_device}'")
    rfxtrx_conn = RFXtrx.Connect(rfxtrx_device, rfxtrx_event_callback, debug=debug)
    return rfxtrx_conn


def shutdown_rfxtrx(rfxtrx_conn):
    log.info("Shutting down RFXtrx")
    rfxtrx_conn.close_connection()


async def run(args):
    config = read_config(args)
    log.info(f"Using config:\n{json.dumps(config, indent=2, sort_keys=True)}")

    # Some config validation
    if config["rfxtrx"]["device"] is None:
        log.error("No RFXtrx device specified")
        sys.exit(1)

    loop = asyncio.get_running_loop()
    try:
        mqtt_client = MQTTClient(client_id=config["mqtt"]["client_id"])
        ret = await mqtt_client.connect(
            f"mqtt://{config['mqtt']['broker_host']}:{config['mqtt']['broker_port']}/",
            cleansession=True)
    except ConnectException as ce:
        log.error("Connection failed: %s" % ce)
        # todo: do what?
        sys.exit(1)

    rfxtrx_conn = await loop.run_in_executor(
        None, setup_rfxtrx, config, loop, mqtt_client, args.debug)

    try:
        while True:
            log_seen_devices(config)
            await asyncio.sleep(60.0)
    except asyncio.CancelledError:
        pass

    await loop.run_in_executor(None, shutdown_rfxtrx, rfxtrx_conn)
    await mqtt_client.disconnect()


# https://stackoverflow.com/a/46020972
def deep_dict_merge(dct1, dct2, override=True) -> dict:
    """
    :param dct1: First dict to merge
    :param dct2: Second dict to merge
    :param override: if same key exists in both dictionaries, should override? otherwise ignore. (default=True)
    :return: The merge dictionary
    """
    merged = copy.deepcopy(dct1)
    for k, v2 in dct2.items():
        if k in merged:
            v1 = merged[k]
            if isinstance(v1, dict) and isinstance(v2, collections.abc.Mapping):
                merged[k] = deep_dict_merge(v1, v2, override)
            elif isinstance(v1, list) and isinstance(v2, list):
                merged[k] = v1 + v2
            else:
                if override:
                    merged[k] = copy.deepcopy(v2)
        else:
            merged[k] = copy.deepcopy(v2)
    return merged


def read_config(args):
    config = {}
    config = deep_dict_merge(config, DEFAULT_CONFIG)

    if args.config_file:
        with open(args.config_file) as f:
            file_config = yaml.safe_load(f)
            config = deep_dict_merge(config, file_config)

    config = deep_dict_merge(config, args_to_config(args))

    # Important: All possible options must be expressed in the default
    # config dict, as usage of the config dict assumes the keys
    # exist. This is currently the case because DEFAULT_CONFIG has all
    # options, but that doesn't feel solid.
    return config


def args_to_config(args):
    # Note that not all args are expressed in the config (for example,
    # --config-file).

    args_config = collections.defaultdict(dict)
    if args.rfxtrx_device:
        args_config["rfxtrx"]["device"] = args.rfxtrx_device

    return args_config


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--config-file", help="Config file")

    parser.add_argument("--rfxtrx-device", help="RFXtrx device")

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
