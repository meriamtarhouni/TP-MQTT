import paho.mqtt.client as paho
from datetime import datetime
import threading
import time

broker_address ="localhost"
port = 1883
timelive=60


def on_connect1(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/connsommationTopic',qos=2)

def on_connect2(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/productionTopic',qos=2)


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
        client = paho.Client("MDMS_Client1")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime(" %H:%M:%S ")
            msg= "Price Data sent at " + current_time
            time.sleep(60)

            result = client.publish("/priceTopic", msg,qos=0)

class publishReductionThread(threading.Thread):
    
    def run(self):
        client = paho.Client("MDMS_Client2")
        client.on_publish = on_publish
        client.connect(broker_address,port)
        while True:
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            msg= "Reduction Data sent at" + current_time
            time.sleep(1)

            result = client.publish("/reductionTopic", msg,qos=1)

try:
    
    thread1=publishPriceThread()
    thread2=publishReductionThread()
    thread3=recieveConsommationThread()
    thread4=recieveProductionThread()
  
 
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

except:

    print ("Error: unable to start thread")