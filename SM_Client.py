import paho.mqtt.client as paho
from datetime import datetime
import threading
import time

broker_address ="localhost"
port = 1883
timelive=60

def on_connect1(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/priceTopic')

def on_connect2(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/reductionTopic')

def on_message(client, userdata, msg):
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(current_time+" "+msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, result):
    print("Data published by SM")


class publishConsommationThread(threading.Thread):

    def run(self):
        client = paho.Client("SM_Client1")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            msg= "Connsommation Data sent at" + current_time
            time.sleep(60)

            result = client.publish("/connsommationTopic", msg)


class publishProductionThread(threading.Thread):
    
    def run(self):
        client = paho.Client("SM_Client2")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            msg= "Production Data sent at" + current_time
            time.sleep(15)

            result = client.publish("/productionTopic", msg)


class recievePriceThread(threading.Thread):
    def run(self):
        time.sleep(10)
        client = paho.Client()
        client.connect(broker_address, port, timelive)
        client.on_connect = on_connect1
        client.on_message=on_message
        client.loop_forever()

class recieveReductionThread(threading.Thread):
    def run(self):
        time.sleep(10)
        client = paho.Client()
        client.connect(broker_address, port, timelive)
        client.on_connect = on_connect2
        client.on_message=on_message
        client.loop_forever()

try:

    thread1=recievePriceThread()
    thread2=recieveReductionThread()
    thread3=publishProductionThread()
    thread4=publishConsommationThread()
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

except:

    print ("Error: unable to start thread")