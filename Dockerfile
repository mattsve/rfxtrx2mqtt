FROM balenalib/armv7hf-debian-python:3.7.4-stretch-run

WORKDIR /

RUN mkdir /rfxtrx2mqtt
COPY * /rfxtrx2mqtt/

RUN mkdir /venv
RUN python3 -m venv /venv
RUN /venv/bin/pip install --upgrade pip
RUN /venv/bin/pip install -r /rfxtrx2mqtt/requirements.txt

ENTRYPOINT [ "/venv/bin/python3", "-u", "/rfxtrx2mqtt/rfxtrx2mqtt.py" ]
