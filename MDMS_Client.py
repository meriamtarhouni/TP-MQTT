import paho.mqtt.client as paho
from datetime import datetime
import threading

broker_address ="localhost"
port = 1883


def on_connect1(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/connsommationTopic')

def on_connect2(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/productionTopic')


def on_message(client, userdata, msg):
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(current_time+" "+msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, result):
    print("Data published by MDMS")



class recieveConsommationThread(threading.Thread):
    def run(self):
        time.sleep(10)
        client = paho.Client()
        client.connect(broker_address, port, timelive)
        client.on_connect = on_connect1
        client.on_message=on_message
        client.loop_forever()


class recieveProductionThread(threading.Thread):
    def run(self):
        time.sleep(10)
        client = paho.Client()
        client.connect(broker_address, port, timelive)
        client.on_connect = on_connect2
        client.on_message=on_message
        client.loop_forever()


class publishPriceThread(threading.Thread):
    
    def run(self):
        client = paho.Client("MDMS_Client")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            msg= "Price Data sent at" + current_time
            time.sleep(60)

            result = client.publish("/priceTopic", msg)

class publishReductionThread(threading.Thread):
    
    def run(self):
        client = paho.Client("MDMS_Client")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            msg= "Reduction Data sent at" + current_time
            time.sleep(1)

            result = client.publish("/reductionTopic", msg)
