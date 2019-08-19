# rfxtrx2mqtt

rfxtrx2mqtt is a program for sending sensor events from an RFXtrx
(http://www.rfxcom.com/) device to Home Assistant via MQTT. Supports
Home Assistant's MQTT discovery to automatically add devices and
sensors.

This is currently only an experiment for personal use and learning. I
only have a few temperature and humidity devices to test with.

The reason for this program was to be able to use and configure the
devices and sensors in the new integrations UI and device registry,
and to make it possible to set Areas for devices. The problem with the
rfxtrx integration in Home Assistant is that it does not yet support
the new device registry, so there are no devices that can have an
area set.

Note that device and sensor IDs are not the same as in the rfxtrx
integration in Home Assistant.


## Dependencies

rfxtrx2mqtt requires pyRFXtrx, hbmqtt and PyYAML.


## Authors

Oskar Skoog <oskar@osd.se>


## Copyright and license

Copyright (c) 2019 Oskar Skoog. rfxtrx2mqtt is provided under the MIT
license, see the included LICENSE file.
