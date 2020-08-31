FROM python:3.8.5-alpine

WORKDIR /
RUN mkdir /rfxtrx2mqtt

WORKDIR /rfxtrx2mqtt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python", "./rfxtrx2mqtt.py" ]
