#python 3.6

#importing required libraries
from paho.mqtt import client as mqtt_client
import json
import argparse
import sys
import requests
import os
import yaml


#scrc tree function 
def scrc_tree(config_path, **kwargs):
    return SCRC_Tree(config_path, **kwargs)


#SCRC class to initiate the functions 
class SCRC_Tree(object):
    def __init__(self, config_path, **kwargs):
       super(SCRC_Tree, self).__init__(**kwargs)

       try:
           self.config = load_config(config_path)
       except Exception as e:
           print(e)

       #get nodes from the config file
       self._nodes = self.config.get('nodes', {})

       #get nodes from the containers
       self._dc = self.config.get('data_containers', {})

       #get configuration params of onem2m
       self._config_onem2m = self.config.get('onem2m' , {
           "cse_ip" : "127.0.0.1",
           "cse_port" : "2000",
           "https" : False
       })

       self._cse_ip = self._config_onem2m['cse_ip']
       self._cse_port = self._config_onem2m['cse_port']
       self._https = self._config_onem2m['https']

       self._mn = self._config_onem2m.get('mn', '/~/in-cse/in-name/')
       self._mn_lbl = self._config_onem2m['mn_lbl']
       self._in = self._config_onem2m.get('in', '/~/in-cse/in-name/')


       self._ae = self._config_onem2m['ae']
       self._ae_lbl = self._config_onem2m['ae_lbl']

       self._api = self._config_onem2m['api']
       self._acpi = self._config_onem2m['acpi']

       self._config_thinkspeak = self.config.get('thingspeak' , {
           "topic" : "channels/1654489/subscribe",
            "client_id" : "PSIVFyMDFjEwNjoXMiEiKQQ",
            "username" : "PSIVFyMDFjEwNjoXMiEiKQQ",
            "password" : "paGEDytVnkcb+LVwRGhmsOLz",
            "port" : "1883" ,
            "broker" : "mqtt3.thingspeak.com",
       })

       self._broker = self._config_thinkspeak['broker']
       self._topic = self._config_thinkspeak['topic']
       self._client_id = self._config_thinkspeak['client_id']
       self._username = self._config_thinkspeak['username']
       self._password = self._config_thinkspeak['password']
       self._port = self._config_thinkspeak['port']

       return

    def connectsd(self):
        print("hellp")

        def connect_mqtt() -> mqtt_client:
            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    print("Connected to MQTT Broker!")
                else:
                    print("Failed to connect, return code %d\n", rc)
            '''
            client = mqtt_client.Client(self._broker)
            print("a")
            client.username_pw_set(self._username, self._password)
            print("b")
            client.on_connect = on_connect
            print("c")
            client.connect(self._broker, int(self._port))
            print("d")
            '''

            broker = 'mqtt3.thingspeak.com'
            port = 1883
            topic = "channels/1654489/subscribe"
            client_id = 'PSIVFyMDFjEwNjoXMiEiKQQ'
            username = 'PSIVFyMDFjEwNjoXMiEiKQQ'
            password = '4jQoRBw7m+Wb0ZtuzBct5ZgH'

            client = mqtt_client.Client(broker)
            print("a")
            client.username_pw_set(username, password)
            print("b")
            client.on_connect = on_connect
            print("c")
            client.connect(broker, int(port))
            print("d")
            subscribe(client)
            client.loop_forever()
            return client

        def subscribe(client: mqtt_client):
            def on_message(client, userdata, msg):
                json_string = msg.payload.decode()
                data = json.loads(json_string)
                inch = data["field1"]
                meter = data["field2"]
                cm = data["field3"]
                datalist = [inch, meter, cm]

                payload = {"m2m:cin": {"lbl": ["label1", "label2", "label3"], "con": str(datalist)}}

                headers = {
                    'X-M2M-Origin': 'admin:admin',
                    'Content-Type': 'application/json;ty=4'
                }
                response = requests.request("POST", self._server, headers=headers, json=payload)
                print(response.text)
            print("after resp")
            client.subscribe(self._topic)
            print("client.sub")
            client.on_message = on_message
            print("clien.msh")

        connect_mqtt()


def get_server_addr(cse_ip, cse_port='', in_cse='/~/in-cse/in-name/', https=True):
    """
    :param cse_ip: str
    :param cse_port: str
    :param in: str
    :param https: bool
    :return: url: str
    @param cse_ip:
    @param cse_port:
    @param https:
    @param in_cse:
    """
    return (
            'http' + ('://' if not https else 's://')
            + cse_ip
            + (':' + cse_port if cse_port is not None else '')
            + (in_cse if in_cse is not None else '/~/in-cse/in-name/')
    )



def load_config(config_path):
    """Load a JSON-encoded configuration file."""
    if config_path is None:
        return {}

    if not os.path.exists(config_path):
        return {}

    # First attempt parsing the file with a yaml parser
    # (allows comments natively)
    # Then if that fails we fallback to our modified json parser.
    try:
        with open(config_path) as f:
            return yaml.safe_load(f.read())
    # except yaml.scanner.ScannerError as e:
    except yaml.YAMLError as e:
        raise Exception("Startup_error: Error Loading Config Files")





#defining the parse arguments config and action 
def parse_argv():
    parser = argparse.ArgumentParser(description='Main ')

    parser.add_argument('--config', metavar='path', required=True, help='config file of the node')
    parser.add_argument('--action', metavar='default subscribe', required=False, help='action to perform [subscribe]')
    args = parser.parse_args()

    return args

#main function which takes the input of the arguments 
def main(argv=None):
    if argv is None:
        argv = sys.argv
    print('main() , argv: {}'.format(argv))

    args = parse_argv()
    config_path = args.config
    action = args.action

    try:
        wmt = scrc_tree(config_path)
        if action is None or action == 'subscribe':
            print('Subscribing.....')
            client = wmt.connectsd()
            #wmt.subscribe(client)
            #client.loop_forever()
    
    except Exception as e:
        print(e)
        print("unhandled exception")
        return -1
    return 0

#initialtes the program
def _main():
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("recived Keyboard interrupt")
        pass
        
#start
if __name__ == '__main__' :
    main()
