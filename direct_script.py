# python3.6

import json
from paho.mqtt import client as mqtt_client
import requests

#MQTT Details and connectivity details 
broker = 'mqtt3.thingspeak.com'
port = 1883
topic = "channels/1654489/subscribe"
client_id = 'PSIVFyMDFjEwNjoXMiEiKQQ'
username = 'PSIVFyMDFjEwNjoXMiEiKQQ'
password = '4jQoRBw7m+Wb0ZtuzBct5ZgH'
url = "http://127.0.0.1:2000/~/in-cse/in-name/ThingSpeak-1654489/Channel-1-1654489/Data"


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, fc):
        if fc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", fc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        json_string = msg.payload.decode()
        data = json.loads(json_string)
        inch = data["field1"]
        meter= data["field2"]
        cm=data["field3"]
        datalist= [inch,meter,cm]
        
        payload={"m2m:cin": {"lbl": ["label1", "label2", "label3"], "con": str(datalist)}}
        
        headers = {
                    'X-M2M-Origin': 'admin:admin',
                    'Content-Type': 'application/json;ty=4'
                    }
        response = requests.request("POST", url, headers=headers, json=payload)
        print(response.text)

       
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
