import communicator
import configparser
import logging
import sys
import paho.mqtt.client as mqtt
import signal
import time

def millis(): return int(round(time.time() * 1000))

class GoodWeProcessor(object):
  def __init__(self) -> None:
    self.shutdown = False
    self.config = configparser.ConfigParser()
    self.config.read('/etc/goodwe.conf')

    self.setup_polling()
    self.setup_logger()

  def setup_polling(self) -> None:
    self.poll_interval = self.config.getint(
        "inverter", "pollinterval", fallback=2500)

  def setup_logger(self) -> None:
    loglevel = self.config.get("inverter", "loglevel", fallback="INFO")

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
      raise ValueError('Invalid log level: %s' % loglevel)

    logging.basicConfig(format='%(asctime)-15s %(funcName)s(%(lineno)d) - %(levelname)s: %(message)s',
                        stream=sys.stderr, level=numeric_level)

  def setup_mqtt_client(self) -> None:
    mqtt_server = self.config.get("mqtt", "server", fallback="localhost")
    mqtt_port = self.config.getint("mqtt", "port", fallback=1883)
    mqtt_clientid = self.config.get("mqtt", "clientid", fallback="goodwe-usb")
    mqtt_username = self.config.get("mqtt", "username", fallback="")
    mqtt_passwd = self.config.get("mqtt", "password", fallback=None)

    self.mqtt_topic = self.config.get("mqtt", "topic", fallback="goodwe")
    try:
      self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, mqtt_clientid)
      if mqtt_username != "":
        self.mqtt_client.username_pw_set(mqtt_username, mqtt_passwd)
        logging.debug("Set username -%s-, password -%s-",
                      mqtt_username, mqtt_passwd)
      self.mqtt_client.connect(mqtt_server, port=mqtt_port)
      self.mqtt_client.loop_start()
    except Exception as e:
      logging.error("%s:%s: %s", mqtt_server, mqtt_port, e)
      return 3
    logging.info('Connected to MQTT %s:%s', mqtt_server, mqtt_port)

  def setup_inverter(self) -> None:
    serialPort = self.config.get("inverter", "serialPort", fallback="/dev/ttyUSB0")
    self.goodwe_communicator = communicator.GoodWeCommunicator(serialPort)

  def run_process(self):
    last_update = millis()
    while True and not self.shutdown:
      try:
        self.goodwe_communicator.handle()

        if (millis() - last_update) > (self.poll_interval * self.goodwe_communicator.get_backoff_multiplier()):
          inverter = self.goodwe_communicator.get_inverter()
          if inverter.serial:
            logging.debug("Checking inverter complete: %s", inverter.serial)

            combinedtopic = self.mqtt_topic + '/' + inverter.serial
            datagram = inverter.to_json()
            if hasattr(self, "mqtt_client"):
              logging.debug('Publishing datagram to MQTT on channel ' + combinedtopic + '/data')
              self.mqtt_client.publish(combinedtopic + '/data', datagram)
              logging.debug('Publishing 1 to MQTT on channel ' + combinedtopic + '/online')
              self.mqtt_client.publish(combinedtopic + '/online', inverter.is_online)
            else:
              logging.warning("MQTT client is not configured")

          last_update = millis()

        time.sleep(10)

      except Exception:
        logging.exception("Error in RUN-loop")
        break

    return 1

  def stop_process(self):
    self.shutdown = True
    if hasattr(self, "mqtt_client"):
      self.mqtt_client.loop_stop()
    if hasattr(self, "goodwe_communicator"):
      self.goodwe_communicator.close_device()


if __name__ == "__main__":
  processor = GoodWeProcessor()

  def signal_handler(sig, frame):
    logging.info("Signal received. Exiting gracefully...")
    processor.stop_process()
    sys.exit(0)

  signal.signal(signal.SIGINT, signal_handler)

  processor.setup_mqtt_client()
  processor.setup_inverter()

  retval = processor.run_process()
  sys.exit(retval)
