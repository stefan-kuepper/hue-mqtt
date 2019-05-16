import socket
import sys
import time
import traceback
import queue
import logging
import json
import websocket
import paho.mqtt.client as mqtt
from threading import Thread
from threading import Timer
from phue import Bridge

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Error(Exception):
    def __init__(self, message):
        self.message = message

class HueError(Error):
    pass


class HueMqttServer:
    config = None
    bridge = None
    mqtt_connected = False
    mqtt_client = None
    status = { 'groups': {}, 'lights': {}, 'sensors':{}, 'scenes':{}}

    bridge_worker = None
    bridge_timer = None
    poll_time=60
    
    ws = None
    ws_worker = None
    
    class BridgeThread(Thread):
        bridge_queue = queue.Queue()
        def run(self):
            self.run=True
            while self.run:
                job = self.bridge_queue.get(block=True)
                logger.info("Bridge work: " + str(job))
                if(job=="quit"):
                    run=False
                else:
                    job.start()
                    job.join() 
                self.bridge_queue.task_done()

    def __init__(self, config):
        self.config = config
        self.bridge_worker = self.BridgeThread()
        self.bridge_worker.setDaemon(True)
        self.bridge_worker.start()

    def mqtt_connect(self):
        if self.mqtt_broker_reachable():
            logger.info('Connecting to ' + self.config['mqtt_host'] + ':' + self.config['mqtt_port'])
            self.mqtt_client = mqtt.Client(self.config['mqtt_client_id'])
            if 'mqtt_user' in self.config and 'mqtt_password' in self.config:
                self.mqtt_client.username_pw_set(self.config['mqtt_user'], self.config['mqtt_password'])

            self.mqtt_client.on_connect = self.mqtt_on_connect
            self.mqtt_client.on_disconnect = self.mqtt_on_disconnect
            self.mqtt_client.on_message = self.mqtt_on_message
            self.mqtt_client.on_subscribe = self.mqtt_on_subscribe
            self.mqtt_client.enable_logger()
            self.mqtt_client.will_set(self.config['mqtt_topic_prefix'] + "/connected", "0", 1, True)

            try:
                self.mqtt_client.connect(self.config['mqtt_host'], int(self.config['mqtt_port']), 10)
            except:
                logger.error(traceback.format_exc())
                self.mqtt_client = None
        else:
            logger.error(self.config['mqtt_host'] + ':' + self.config['mqtt_port'] + ' not reachable!')
    
    def mqtt_on_subscribe(self, client, userdata, mid, granted_qos):
        logger.info('Subscribed. Message id=' + str(mid))
        
    def mqtt_on_connect(self, mqtt_client, userdata, flags, rc):
        logger.info('...mqtt_connected!')
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/get/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/get/#', self.mqtt_on_message_get)
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/set/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/set/lights/#', self.mqtt_on_message_set_light)
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/command/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/command/#', self.mqtt_on_message_command)

        self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", "1", 1, True)
        self.bridge_worker.bridge_queue.put(Thread(target=self.bridge_connect))

    def mqtt_on_disconnect(self, mqtt_client, userdata, rc):
        logger.warn('Diconnected! will reconnect! ...')
        if rc is 0:
            self.mqtt_connect()
        else:
            time.sleep(5)
            while not self.mqtt_broker_reachable():
                time.sleep(10)
            self.mqtt_client.reconnect()
            
    def mqtt_on_message_get(self, client, userdata, message):
        topic = message.topic.split("/")
        if topic[2] in self.status:
            for dev in self.status[topic[2]]:
                if self.status[topic[2]][dev]['name'] == topic[3]:
                    logger.info("Get request for {} recieved".format(
                                topic[3]))
                    topic_prefix = "{}/{}/{}/{}".format(
                            self.config['mqtt_topic_prefix'],
                            'status', topic[2], topic[3] )
                    msg = json.dumps(self.status[topic[2]][dev])
                    self.mqtt_client.publish(topic_prefix, msg, 0 , True)
                    break
                    
    def mqtt_on_message_set_light(self, client, userdata, message):  
        topic = message.topic.split("/")
        payload = json.loads(message.payload.decode("utf-8"))
        light = self.bridge.get_light(topic[3]) 
        if light:
            if len(topic) == 4 or (len(topic) == 5 and topic[4]) == '':
               # hue/set/light/*name* or hue/set/light/*name*/
               import pdb;pdb.set_trace()
               self.BridgeThread.bridge_queue.put(
                        Thread(
                            target=self.bridge.set_light, 
                            args=(topic[3], payload)))
            elif len(topic) == 5:
                value = None
                if topic[4] == "on": 
                    #import pdb;pdb.set_trace()
                    if payload.lower() in ("0","false","off"):
                        value = False
                    elif payload.lower() in ("1","true","on"):
                        value = True
                elif topic[4] in ("bri", "ct", "sat", "hue"):
                    value = int(message.payload)
                
                if(value != None):
                    logger.info('Set light "' + topic[3] + '" ' + topic[4] + ' to ' + str(value))
                    self.BridgeThread.bridge_queue.put(Thread(target=self.bridge.set_light, args=(light['name'], topic[4], value)))
                else:
                    logger.warn('Wrong value for light "' + topic[3] + "/" + topic[4] + '" : ' + payload)
                        
    def mqtt_on_message_command(self,client, userdata, message):
        topic = message.topic.split("/")
        if topic[2] == "scan_sensors":
            logger.info("Scanning for new sensors")
            self.BridgeThread.bridge_queue.put(
                    Thread(target=self.bridge.scan_sensors))
        elif topic[2] == "scan_lights":
            logger.info("Scanning for new lights")
            self.BridgeThread.bridge_queue.put(
                    Thread(target=self.bridge.scan_sensors))
    
    def mqtt_on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8")
        logger.info("MQTT message: " + topic  + ": " + payload)


    def mqtt_broker_reachable(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        try:
            s.connect((self.config['mqtt_host'], int(self.config['mqtt_port'])))
            s.close()
            return True
        except socket.error:
            return False

    def bridge_connect(self):
        logger.info("bridge_connect")
        self.bridge = Bridge(ip=self.config['bridge_ip'], username=self.config['bridge_username'])
        response = self.bridge.get_api()
        if response.__class__ == list and 'error' in response[0]:
            raise HueError("Connection to bridge failed: " + response[0]['error']['description'])
        else:
            logger.info('Bridge connected!')
            if self.bridge_timer:
                self.bridge_timer.cancel()
            
            if 'websocketport' in response['config']:
                ws = websocket.WebSocketApp("ws://{}:{}/".format(
                                        self.config['bridge_ip'],
                                        response['config']['websocketport']),
                              on_message = self.ws_on_message)
                              #on_error = on_error,
                              #on_close = on_close)
                ws_worker = Thread(target = ws.run_forever, 
                            name = "websocket", daemon = True)
                ws_worker.start()
            
            self.update_bridge()
            self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", "2", 1, True)
            
    def ws_on_message(self, message):
        logger.debug("Recieved WS message: {}".format(message))
        # Update device
        msg = json.loads(message)
        if msg['e'] == 'changed':
            dev = self.status[msg['r']][msg['id']]
            if 'state' in msg:
                ts = int(time.time()*1000)
                dev['state'] = msg['state']
                dev['val'] = self._get_val(msg['r'], dev['type'], 
                                                 dev['state'])
                dev['lc'] = dev['ts']
                dev['ts'] = ts
                topic_prefix = ( self.config['mqtt_topic_prefix'] 
                                 + '/status/' + msg['r'] + '/' + dev['name'] )
                msg = json.dumps(dev)
                self.mqtt_client.publish(topic_prefix, msg, 0 , True)
        
    def update_bridge(self):
        logger.debug('Bridge update')
        if self.bridge_timer:
            self.bridge_timer.cancel() # 
        self.publish_status()
        self.bridge_timer = Timer(self.poll_time, self.update_bridge)
        self.bridge_timer.start()
    
    def _get_val(self, res, _type, state):
        val = -1
        if res == 'lights':
            val = state['bri']
        elif res == 'groups':
            if state['any_on']:
                if state['all_on']:
                    val = "all_on"
                else:
                    val = "any_on"
            else:
                val = "none_on"
        elif res == 'sensors':
            if _type == 'ZHASwitch':
                val = state['buttonevent']
            elif _type == 'Daylight':
                val = state['daylight']
            else:
                logger.warn("Unknown sensor type")
        
        return val

    def publish_status(self):
        api=self.bridge.get_api()
        for res in ('groups', 'lights', 'sensors'):
            for dev_id in api[res]:
                dev = api[res][dev_id]
                state = {}
                
                if 'etag' in dev:
                    if (dev_id in self.status[res]
                        and 'etag' in self.status[res][dev_id]
                        and self.status[res][dev_id]['etag'] == dev['etag']):
                            # device has not changed
                            continue
                    else:
                        state['etag'] = dev['etag']
                ts = int(time.time()*1000)
                
                if 'manufacturername' in dev:
                    state['manufacturername'] = dev['manufacturername']
                if 'modelid' in dev:
                    state['modelid'] = dev['modelid']
                if 'name' in dev:
                    state['name'] = dev['name']
                if 'type' in dev:
                    state['type'] = dev['type']
                if 'uniqueid' in dev:
                    state['uniqueid'] = dev['uniqueid']
                if 'state' in dev:
                    state['state'] = dev['state']
                    state['val'] = self._get_val(res, state['type'], 
                                                 dev['state'])
                    
                
                if dev_id not in self.status[res]:
                    logger.info('Discovered new ' + state['type'] 
                                + ': ' + str(state))
                    self.status[res][dev_id] = {}
                    self.status[res][dev_id]['ts'] = ts
            
    
                logger.debug('Status of device "' + dev['name'] + '" changed')
                #import pdb;pdb.set_trace()
                state['lc'] = self.status[res][dev_id]['ts']
                state['ts'] = ts
                
                self.status[res][dev_id] = state
                topic_prefix = ( self.config['mqtt_topic_prefix'] 
                                 + '/status/' + res + '/' + dev['name'] )
                msg = json.dumps(state)
                self.mqtt_client.publish(topic_prefix, msg, 0 , True)


    def start(self):
        self.mqtt_connect()
        try:
            self.mqtt_client.loop_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Quitting")
            self.bridge_worker.bridge_queue.put("quit")
            if self.bridge_timer:
                self.bridge_timer.cancel()
            sys.exit()
        except socket.error:
            logger.error("Lost MQTT connection")
            sys.exit();
