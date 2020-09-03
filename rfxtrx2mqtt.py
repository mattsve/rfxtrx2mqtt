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
from hbmqtt.mqtt.constants import QOS_0
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
        "discovery_prefix": "homeassistant",

        "rfxtrx_device_id_prefix": "rfxtrx2mqtt_rfxtrx_",
        "rfxtrx_device_name_prefix": "rfxtrx_",

        "rfxtrx2mqtt_device_id": "rfxtrx2mqtt",
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
    },

    "emulated_battery_level_sensor_enabled": False,
    "emulated_signal_strength_sensor_enabled": False,
}


# RFXtrx event value keys
EVENT_KEY_TEMPERATURE = "Temperature"
EVENT_KEY_TEMPERATURE_2 = "Temperature2"
EVENT_KEY_HUMIDITY = "Humidity"
EVENT_KEY_WIND_DIRECTION = "Wind direction"
EVENT_KEY_WIND_AVERAGE_SPEED = "Wind average speed"
EVENT_KEY_WIND_GUST = "Wind gust"
EVENT_KEY_BATTERY_NUMERIC = "Battery numeric"
EVENT_KEY_RSSI_NUMERIC = "Rssi numeric"


# Sensor IDs - Unique identifiers for the different sensors
# (also used in Home Assitant entities)
SENSOR_ID_TEMPERATURE = "temperature"
SENSOR_ID_TEMPERATURE_2 = "temperature_2"
SENSOR_ID_HUMIDITY = "humidity"
SENSOR_ID_WIND_DIRECTION = "wind_direction"
SENSOR_ID_WIND_AVERAGE_SPEED = "wind_average_speed"
SENSOR_ID_WIND_GUST = "wind_gust"
SENSOR_ID_BATTERY_NUMERIC = "battery"
SENSOR_ID_RSSI_NUMERIC = "rssi"
SENSOR_ID_BATTERY_LEVEL = "battery_level"
SENSOR_ID_SIGNAL_STRENGTH = "signal_strength"


# Sensor types (also used as Home Assistant device class)
SENSOR_TYPE_TEMPERATURE = "temperature"
SENSOR_TYPE_HUMIDITY = "humidity"
SENSOR_TYPE_WIND_DIRECTION = "wind_direction"
SENSOR_TYPE_WIND_AVERAGE_SPEED = "wind_average_speed"
SENSOR_TYPE_WIND_GUST = "wind_gust"
SENSOR_TYPE_BATTERY_NUMERIC = "battery_numeric"
SENSOR_TYPE_RSSI_NUMERIC = "rssi_numeric"
SENSOR_TYPE_BATTERY_LEVEL = "battery_level"
SENSOR_TYPE_SIGNAL_STRENGTH = "signal_strength"


# Values (the sensor IDs) must be unique.
event_value_key_to_sensor_id_map = {
    EVENT_KEY_TEMPERATURE: SENSOR_ID_TEMPERATURE,
    EVENT_KEY_TEMPERATURE_2: SENSOR_ID_TEMPERATURE_2,
    EVENT_KEY_HUMIDITY: SENSOR_ID_HUMIDITY,
    EVENT_KEY_WIND_DIRECTION: SENSOR_ID_WIND_DIRECTION,
    EVENT_KEY_WIND_AVERAGE_SPEED: SENSOR_ID_WIND_AVERAGE_SPEED,
    EVENT_KEY_WIND_GUST: SENSOR_ID_WIND_GUST,
    EVENT_KEY_BATTERY_NUMERIC: SENSOR_ID_BATTERY_NUMERIC,
    EVENT_KEY_RSSI_NUMERIC: SENSOR_ID_RSSI_NUMERIC,
}


# sensor_type is also used as Home Assistant device class
event_value_key_to_sensor_type_map = {
    EVENT_KEY_TEMPERATURE: SENSOR_TYPE_TEMPERATURE,
    EVENT_KEY_TEMPERATURE_2: SENSOR_TYPE_TEMPERATURE,
    EVENT_KEY_HUMIDITY: SENSOR_TYPE_HUMIDITY,
    EVENT_KEY_WIND_DIRECTION: SENSOR_TYPE_WIND_DIRECTION,
    EVENT_KEY_WIND_AVERAGE_SPEED: SENSOR_TYPE_WIND_AVERAGE_SPEED,
    EVENT_KEY_WIND_GUST: SENSOR_TYPE_WIND_GUST,
    EVENT_KEY_BATTERY_NUMERIC: SENSOR_TYPE_BATTERY_NUMERIC,
    EVENT_KEY_RSSI_NUMERIC: SENSOR_TYPE_RSSI_NUMERIC,
}


sensor_type_to_unit_of_measurement_map = {
    SENSOR_TYPE_TEMPERATURE: "°C",
    SENSOR_TYPE_HUMIDITY: "%",
    SENSOR_TYPE_WIND_DIRECTION: "°",
    SENSOR_TYPE_WIND_AVERAGE_SPEED: "m/s",
    SENSOR_TYPE_WIND_GUST: "m/s",
    SENSOR_TYPE_BATTERY_NUMERIC: None,
    SENSOR_TYPE_RSSI_NUMERIC: None,
    SENSOR_TYPE_BATTERY_LEVEL: "%",
    SENSOR_TYPE_SIGNAL_STRENGTH: "%",
}

# Sensor type to Home Assistant device class (only some sensor types
# have a defined device class). HA does not have device classes for
# "battery numeric" or "rssi numeric".
#
# https://developers.home-assistant.io/docs/en/entity_sensor.html
sensor_type_to_device_class_map = {
    SENSOR_TYPE_TEMPERATURE: "temperature",
    SENSOR_TYPE_HUMIDITY: "humidity",
    SENSOR_TYPE_BATTERY_LEVEL: "battery",
    SENSOR_TYPE_SIGNAL_STRENGTH: "signal_strength",
}


# It's not actually possible to get a realistic percentage, but it's
# more convenient to use Home Assistant built-in representation of
# device battery level.
def battery_numeric_to_battery_level(value):
    max_value = 9
    return int((value / max_value) * 100)


# RSSI is not actually signal strength, but it's convenient to see
# this as a signal strength percentage value.
def rssi_numeric_to_signal_strength(value):
    max_value = 10
    return int((value / max_value) * 100)


def get_discovery_topic(component, device_id, sensor_id, config):
    assert component == "sensor", "rfxtrx2mqtt only supports sensor components yet"
    # Sensor ID is the same as object_id in the Home Assistant MQTT discovery topic format.
    # Discovery topic format: <discovery_prefix>/<component>/[<node_id>/]<object_id>/config
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
def create_device_id(pyrfxtrx_device):
    packettype = f"{pyrfxtrx_device.packettype:02x}"
    subtype = f"{pyrfxtrx_device.subtype:02x}"
    id_string = pyrfxtrx_device.id_string.replace(":", "")
    return f"{packettype}{subtype}_{id_string}"


class Device:
    def __init__(self, *, device_id, packettype, subtype, id_string, type_string, sensors):
        self.device_id = device_id
        self.packettype = packettype
        self.subtype = subtype
        self.id_string = id_string
        self.type_string = type_string
        self.sensors = sensors


class Sensor:
    def __init__(self, *, sensor_id, sensor_type):
        # The sensor id is relative to the device
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.unit_of_measurement = sensor_type_to_unit_of_measurement_map[sensor_type]


def get_sensors(event, config):
    sensors = {}
    for key in event.values:
        if key not in event_value_key_to_sensor_id_map:
            log.debug(f"No sensor ID for event value key: {key}")
            continue
        if key not in event_value_key_to_sensor_type_map:
            log.debug(f"No sensor type for event value key: {key}")
            continue

        sensor_id = event_value_key_to_sensor_id_map[key]
        sensors[sensor_id] = Sensor(
            sensor_id=sensor_id,
            sensor_type=event_value_key_to_sensor_type_map[key])

        if key == EVENT_KEY_BATTERY_NUMERIC and config['emulated_battery_level_sensor_enabled']:
            sensors[SENSOR_ID_BATTERY_LEVEL] = Sensor(
                sensor_id=SENSOR_ID_BATTERY_LEVEL,
                sensor_type=SENSOR_TYPE_BATTERY_LEVEL)

        if key == EVENT_KEY_RSSI_NUMERIC and config['emulated_signal_strength_sensor_enabled']:
            sensors[SENSOR_ID_SIGNAL_STRENGTH] = Sensor(
                sensor_id=SENSOR_ID_SIGNAL_STRENGTH,
                sensor_type=SENSOR_TYPE_SIGNAL_STRENGTH)

    return sensors


def event_values_to_state(values, config):
    state = {}
    for key in values:
        if key not in event_value_key_to_sensor_id_map:
            log.debug(f"No sensor ID for event value key: {key}")
            continue
        if key not in event_value_key_to_sensor_type_map:
            log.debug(f"No sensor type for event value key: {key}")
            continue

        sensor_id = event_value_key_to_sensor_id_map[key]
        state[sensor_id] = values[key]

        if key == EVENT_KEY_BATTERY_NUMERIC and config['emulated_battery_level_sensor_enabled']:
            state[SENSOR_ID_BATTERY_LEVEL] = battery_numeric_to_battery_level(values[key])

        if key == EVENT_KEY_RSSI_NUMERIC and config['emulated_signal_strength_sensor_enabled']:
            state[SENSOR_ID_SIGNAL_STRENGTH] = rssi_numeric_to_signal_strength(values[key])

    return state


def create_device(event, config):
    device_id = create_device_id(event.device)
    sensors = get_sensors(event, config)
    device = Device(
        device_id=device_id,
        packettype=event.device.packettype,
        subtype=event.device.subtype,
        id_string=event.device.id_string,
        type_string=event.device.type_string,
        sensors=sensors)
    return device


def get_device_name(device, config):
    default = f"{config['homeassistant']['rfxtrx_device_name_prefix']}{device.device_id}"
    return config["device_name_map"].get(device.device_id, default)


def get_sensor_name(device, sensor, config):
    device_prefix = config["sensor_name_map"].get(device.device_id, f"rfxtrx_{device.device_id}")
    return f"{device_prefix}_{sensor.sensor_id}"


def create_rfxtrx_device_config(device, config):
    device_config = {
        "name": get_device_name(device, config),
        "identifiers": [f"{config['homeassistant']['rfxtrx_device_id_prefix']}{device.device_id}"],
        "manufacturer": f"{device.type_string}",
        "via_device": f"{config['homeassistant']['rfxtrx2mqtt_device_id']}",
    }
    return device_config


def create_rfxtrx_sensor_config(device, sensor, config):
    component = "sensor"
    sensor_config = {
        "name": get_sensor_name(device, sensor, config),
        "unique_id": f"{config['homeassistant']['rfxtrx_device_id_prefix']}{device.device_id}_{sensor.sensor_id}",
        "state_topic": get_state_topic(component, device.device_id, config),
        "value_template": f"{{{{ value_json.{sensor.sensor_id} }}}}",
        "device": create_rfxtrx_device_config(device, config),
    }

    if sensor.unit_of_measurement:
        sensor_config["unit_of_measurement"] = sensor.unit_of_measurement

    # Only some sensor types have correct device classes in Home Assistant.
    if sensor.sensor_type in sensor_type_to_device_class_map:
        sensor_config["device_class"] = sensor_type_to_device_class_map[sensor.sensor_type]

    return sensor_config


async def publish_rfxtrx_discovery(client, device, config):
    component = "sensor"
    for sensor_id, sensor in device.sensors.items():
        topic = get_discovery_topic(component, device.device_id, sensor_id, config)
        sensor_config = create_rfxtrx_sensor_config(device, sensor, config)
        log.debug(f"Publishing discovery config '{sensor_config}' for device '{device.device_id}' on topic '{topic}'")
        msg = json.dumps(sensor_config)
        # (retain and qos=0 is what zigbee2mqtt does)
        #
        # FIXME: The downside is that when experimenting with the
        # setup, rfxtrx2mqtt might send discovery configs that you
        # don't want to use later, but those will be retained
        # anyway. (I think that restarting the MQTT broker will drop
        # the retained messages.)
        retain = True
        await client.publish(topic, msg.encode("utf-8"), retain=retain, qos=QOS_0)


async def publish_rfxtrx_state(client, device_id, event, config):
    component = "sensor"
    topic = get_state_topic(component, device_id, config)
    state = event_values_to_state(event.values, config)
    log.debug(f"Publishing state '{state}' for device_id '{device_id}' on topic '{topic}'")
    msg = json.dumps(state)
    await client.publish(topic, msg.encode("utf-8"))


async def handle_event(event, mqtt_client, config):
    try:
        log.debug(f"Got event {event.__dict__}) from device {event.device.__dict__}")
        if not isinstance(event, RFXtrx.SensorEvent):
            log.info(f"Ignoring event, not a sensor event! Event: {event}")
            return

        device = create_device(event, config)

        whitelisted_device_ids = [ w["device_id"] for w in config["whitelist"] ]
        if not config["whitelist"]:
            is_whitelisted = True
        elif device.device_id in whitelisted_device_ids:
            is_whitelisted = True
        else:
            is_whitelisted = False

        log.info(f"Event: Device with ID '{device.device_id}' (whitelisted: {is_whitelisted}) sent sensor values: {event.values}")
        if device.device_id not in seen_devices:
            log.info(f"Found new device: Device ID: {device.device_id}, packettype '{device.packettype:02x}', subtype: '{device.subtype:02x}', id_string: '{device.id_string}', type_string: '{device.type_string}', whitelisted: {is_whitelisted}")
            seen_devices[device.device_id] = device
            if is_whitelisted:
                await publish_rfxtrx_discovery(mqtt_client, device, config)
            else:
                log.debug(f"Device ID {device.device_id} not whitelisted, not publishing discovery config")

        seen_devices_last_values[device.device_id] = event.values
        seen_devices_last_timestamp[device.device_id] = time.time()

        # Whitelist is not enabled if it's empty
        if is_whitelisted:
            state = event_values_to_state(event.values, config)
            await publish_rfxtrx_state(mqtt_client, device.device_id, event, config)
        else:
            log.debug(f"Device ID {device.device_id} not whitelisted, not publishing event state")
    except Exception:
        log.exception("Exception in handle_event")


async def publish_rfxtrx2mqtt_discovery(client, config):
    """Publish MQTT discovery config about rfxtrx2mqtt's (this program's)
    own sensors.

    """
    component = "sensor"
    device_id = f"{config['homeassistant']['rfxtrx2mqtt_device_id']}"
    sensor_ids = ["version", "seen_devices_count"]
    for sensor_id in sensor_ids:
        topic = get_discovery_topic(component, device_id, sensor_id, config)
        discovery_config = {
            "name": f"{device_id}_{sensor_id}",
            "unique_id": f"{device_id}_{sensor_id}",
            "state_topic": get_state_topic(component, device_id, config),
            "value_template": f"{{{{ value_json.{sensor_id} }}}}",
            "device": {
                "name": f"{device_id}",
                "identifiers": [f"{device_id}"],
                "sw_version": f"rfxtrx2mqtt {rfxtrx2mqtt_version}",
                "manufacturer": "rfxtrx2mqtt",
            }
        }
        log.debug(f"Publishing discovery config '{discovery_config}' for device '{device_id}' on topic '{topic}'")
        msg = json.dumps(discovery_config)
        # (retain and qos=0 is what zigbee2mqtt does)
        #
        # FIXME: The downside is that when experimenting with the
        # setup, rfxtrx2mqtt might send discovery configs that you
        # don't want to use later, but those will be retained
        # anyway. (I think that restarting the MQTT broker will drop
        # the retained messages.)
        retain = True
        await client.publish(topic, msg.encode("utf-8"), retain=retain, qos=QOS_0)


async def publish_rfxtrx2mqtt_state(client, config):
    component = "sensor"
    device_id = f"{config['homeassistant']['rfxtrx2mqtt_device_id']}"
    topic = get_state_topic(component, device_id, config)
    state = {
        "version": rfxtrx2mqtt_version,
        "seen_devices_count": len(seen_devices),
    }
    log.debug(f"Publishing state '{state}' for device_id '{device_id}' on topic '{topic}'")
    msg = json.dumps(state)
    await client.publish(topic, msg.encode("utf-8"))


def log_seen_rfxtrx_devices(config):
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
                 f"   packettype: {device.packettype:02x}\n"
                 f"   subtype: {device.subtype:02x}\n"
                 f"   id_string: {device.id_string}\n"
                 f"   type_string: {device.type_string}\n"
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
    def rfxtrx_event_callback(event):
        asyncio.run_coroutine_threadsafe(handle_event(event, mqtt_client, config), loop)

    try:
        rfxtrx_device = config["rfxtrx"]["device"]
        log.info(f"Using RFXtrx device '{rfxtrx_device}'")
        # RFXtrx.Connect starts and runs its own thread
        rfxtrx_conn = RFXtrx.Connect(rfxtrx_device, rfxtrx_event_callback, debug=debug)
        return rfxtrx_conn
    except Exception:
        log.exception("Failed to setup RFXtrx device")
        sys.exit(2)


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
        log.error("Connection to MQTT broker failed: %s" % ce)
        # todo: do what?
        sys.exit(1)

    await publish_rfxtrx2mqtt_discovery(mqtt_client, config)

    rfxtrx_conn = await loop.run_in_executor(
        None, setup_rfxtrx, config, loop, mqtt_client, args.debug)

    try:
        while True:
            log_seen_rfxtrx_devices(config)
            await publish_rfxtrx2mqtt_state(mqtt_client, config)
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
    parser.add_argument("--rfxtrx-device", help="RFXtrx USB device (example: /dev/tty.usbserial-A11Q57E2)")
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
