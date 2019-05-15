import socket
import sys
import time
import traceback
import queue
import logging
import json
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
    status = {}
    status_lights = {}
    status_groups = {}
    status_sensors = {}

    bridge_worker = None
    bridge_timer = None
    poll_time=1
    
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
        result, mid = self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/command/#')
        logger.info('Subscribe to: ' + self.config['mqtt_topic_prefix'] + '/command/# Message id=' + str(mid))
        result,mid = self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/set/#')
        logger.info('Subscribe to: ' + self.config['mqtt_topic_prefix'] + '/set/# Message id=' + str(mid))

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

    def mqtt_on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8")
        logger.info("MQTT message: " + topic  + ": " + payload)
        '''
        Possible groups:
        /set/light/*name*/
        /set/light/*name*/on
        /set/light/*name*/bri
        /set/light/*name/ct
        ''' 
        if self.bridge:
            topic = topic.split("/")
            if topic[1] == "set":
                if topic[2] == "light":
                    light = self.bridge.get_light(topic[3]) 
                    if light:
                        if len(topic) == 4 or (len(topic) == 5 and topic[4]) == '':
                           # /set/light/*name*
                           pass
                        elif len(topic) == 5:
                            value = None
                            if topic[4] == "on": 
                                #import pdb;pdb.set_trace()
                                if payload.lower() in ("0","false","off"):
                                    value = False
                                elif payload.lower() in ("1","true","on"):
                                    value = True
                            elif topic[4] in ("bri", "ct", "sat"):
                                value = int(message.payload)
                            
                            if(value != None):
                                logger.info('Set light "' + topic[3] + '" ' + topic[4] + ' to ' + str(value))
                                self.BridgeThread.bridge_queue.put(Thread(target=self.bridge.set_light, args=(light['name'], topic[4], value)))
                                #self.bridge.set_light(light['name'], topic[4], value)
                                self.BridgeThread.bridge_queue.put(Thread(target=self.update_bridge))
                                #self.update_bridge():
                            else:
                                logger.warn('Wrong value for light "' + topic[3] + "/" + topic[4] + '" : ' + payload)
                                
                elif topic[2] == "groups":
                    logger.warn('Groups not implemented')
                    # TODO: Implement groups 
            elif topic[1] == "command":
                if topic[2] == "scan_sensors":
                    logger.info("Scanning for new sensors")
                    self.BridgeThread.bridge_queue.put(Thread(target=self.bridge.scan_sensors))
                elif topic[2] == "scan_lights":
                    logger.info("Scanning for new lights")
                    self.BridgeThread.bridge_queue.put(Thread(target=self.bridge.scan_sensors))
            #logger.info(message.topic + ': ' + str(float(message.payload)))
            #self.bridge_queue.put(Thread(target=self. .set_target_temperature,
             #                          args=(self.device_mapping[message.topic], float(message.payload))))

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

            self.update_bridge()
            self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", "2", 1, True)
        
    def update_bridge(self):
        logger.debug('Bridge update')
        if self.bridge_timer:
            self.bridge_timer.cancel() # 
        self.publish_status()
        self.bridge_timer = Timer(self.poll_time, self.update_bridge)
        self.bridge_timer.start()

    def publish_status(self):
        api=self.bridge.get_api()
        for light_id in api['lights']:
            topic_prefix = self.config['mqtt_topic_prefix'] + '/status/light/' + api['lights'][light_id]['name']
            state = json.dumps({"val": api['lights'][light_id]['state']['bri'],"state": api['lights'][light_id]['state']})
            if light_id not in self.status_lights or self.status_lights[light_id] != state:
                logger.debug('Status of light "' + api['lights'][light_id]['name'] + '" changed')
                self.status_lights[light_id] = state
                self.mqtt_client.publish(topic_prefix, state, 0 , True)

        for group_id in api['groups']:
            topic_prefix = self.config['mqtt_topic_prefix'] + '/status/group/' + api['groups'][group_id]['name']
            state = json.dumps({"val": api['groups'][group_id]['action']['bri'],"id": api['groups'][group_id]['action'],"state": api['groups'][group_id]['state']})
            if group_id not in self.status_groups or self.status_groups[group_id] != state:
                logger.debug('Status of group "' + api['groups'][group_id]['name'] + '" changed')
                self.status_groups[group_id] = state
                self.mqtt_client.publish(topic_prefix, state, 0 , True)
        for sensor_id in api['sensors']:
            sensor = api['sensors'][sensor_id]
            state = {}
            if 'state' in sensor:
                state['state'] = sensor['state']
            if 'name' in sensor:
                state['name'] = sensor['name']
            if 'manufacturername' in sensor:
                state['manufacturername'] = sensor['manufacturername']
            if 'modelid' in sensor:
                state['modelid'] = sensor['modelid']
            if 'battery' in sensor['config']:
                state['battery'] = sensor['config']['battery']
            
            if sensor['type'] == 'ZHASwitch':
                state['val'] = sensor['state']['buttonevent']
            elif sensor['type'] == 'Daylight':
                val = sensor['state']['daylight']
            else:
                logger.warn("Unknown sensor type")
                state['val'] = -1
                
            if sensor_id not in self.status_sensors:
                logger.info('Discovered new sensor: ' + str(state))
                self.status_sensors[sensor_id] = {}
                self.status_sensors[sensor_id]['ts'] = int(time.time()*1000)
            
            changed = False
            for key in state:
                if key not in self.status_sensors[sensor_id]:
                    changed = True
                    break
                elif state[key] != self.status_sensors[sensor_id][key]:
                    changed = True
                    break
    
            if changed:
                logger.debug('Status of sensor "' + sensor['name'] + '" changed')
                #import pdb;pdb.set_trace()
                state['lc'] = self.status_sensors[sensor_id]['ts']
                state['ts'] = int(time.time()*1000)
                
                self.status_sensors[sensor_id] = state
                topic_prefix = ( self.config['mqtt_topic_prefix'] 
                                 + '/status/sensor/' + sensor['name'] )
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
