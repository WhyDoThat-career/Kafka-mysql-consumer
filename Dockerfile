FROM python:3.8

COPY requirements.txt /opt/
RUN pip3 install -r /opt/requirements.txt

WORKDIR /opt
COPY . .

CMD  ["python3","Kafka-mysql-consumer/run.py"]