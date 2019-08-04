# rfxtrx2mqtt

rfxtrx2mqtt is a program for sending sensor events from an RFXtrx
device to Home Assistant via MQTT. Supports MQTT discovery to
automatically add devices and sensors.

This is just experimentation for personal use and learning.

The reason for this program was to be able to use and configure the
devices and sensors in the new integrations UI. The problem with the
rfxtrx integration in Home Assistant is that it does not yet support
the new device registry. I also did not like the IDs that the rfxtrx
integration generates and use for the sensors.
