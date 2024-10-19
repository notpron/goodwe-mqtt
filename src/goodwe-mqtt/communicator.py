from __future__ import absolute_import

import json
import logging
import serial
import time

from enum import IntEnum
from exceptions import InverterError, RequestFailedException, RequestRejectedException, PartialResponseException
from typing import Union


def millis(): return int(round(time.time() * 1000))

def create_crc16_table() -> tuple:
  """Construct (modbus) CRC-16 table"""
  table = []
  for i in range(256):
      buffer = i << 1
      crc = 0
      for _ in range(8, 0, -1):
          buffer >>= 1
          if (buffer ^ crc) & 0x0001:
              crc = (crc >> 1) ^ 0xA001
          else:
              crc >>= 1
      table.append(crc)
  return tuple(table)

CRC_16_TABLE = create_crc16_table()

def modbus_checksum(data: Union[bytearray, bytes]) -> int:
    """
    Calculate modbus crc-16 checksum
    """
    crc = 0xFFFF
    for ch in data:
        crc = (crc >> 8) ^ CRC_16_TABLE[(crc ^ ch) & 0xFF]
    return crc

FAILURE_CODES = {
    1: "ILLEGAL FUNCTION",
    2: 'ILLEGAL DATA ADDRESS',
    3: "ILLEGAL DATA VALUE",
    4: "SLAVE DEVICE FAILURE",
    5: "ACKNOWLEDGE",
    6: "SLAVE DEVICE BUSY",
    7: "NEGATIVE ACKNOWLEDGEMENT",
    8: "MEMORY PARITY ERROR",
    10: "GATEWAY PATH UNAVAILABLE",
    11: "GATEWAY TARGET DEVICE FAILED TO RESPOND",
}

ERRORS = []
ERRORS.append("GFCI Device Failure")
ERRORS.append("AC HCT Failure")
ERRORS.append("TBD")
ERRORS.append("DCI Consistency Failure")
ERRORS.append("GFCI Consistency Failure")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("Utility Loss")
ERRORS.append("Gournd I Failure")
ERRORS.append("DC Bus High")
ERRORS.append("Internal Version Unmatch")
ERRORS.append("Over Temperature")
ERRORS.append("Auto Test Failure")
ERRORS.append("PV Over Voltage")
ERRORS.append("Fan Failure")
ERRORS.append("Vac Failure")
ERRORS.append("Isolation Failure")
ERRORS.append("DC Injection High")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("Fac Consistency Failure")
ERRORS.append("Vac Consistency Failure")
ERRORS.append("TBD")
ERRORS.append("Relay Check Failure")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("TBD")
ERRORS.append("Fac Failure")
ERRORS.append("EEPROM R/W Failure")
ERRORS.append("Internal Communication Failure")

class RunningInfo(object):

  def __init__(self):
    self.vpv1 = 0.0
    self.ipv1 = 0.0
    self.ppv1 = 0.0
    self.vac1 = 0.0
    self.iac1 = 0.0
    self.fac1 = 0.0
    self.pac = 0.0
    self.workMode = 0
    self.temp = 0.0
    self.eTotal = 0.0
    self.eDay = 0.0

  def to_json(self):
    return json.dumps(self.__dict__)


class Inverter(object):

  def __init__(self):
    self.serial = ""                # serial number as string
    self.modbus_address = 0xF7      # address provided by this software
    self.is_online = False          # is the inverter online (see above)
    self.running_info = RunningInfo()

  def to_json(self):
    return self.running_info.to_json()
  
  def read_int(self, buffer: bytes, register_offset: int) -> int:
    """Retrieve 2 byte (unsigned int) value from buffer"""
    byte_offset = register_offset * REGISTER_RUNNING_DATA_SIZE
    value = int.from_bytes(buffer[byte_offset:byte_offset + REGISTER_RUNNING_DATA_SIZE], byteorder="big", signed=False)
    return 0 if value == 0xffff else value
  
  def read_float(self, buffer: bytes, gain: int, register_offset: int) -> float:
    """Retrieve value (2 unsigned bytes) from buffer"""
    byte_offset = register_offset * REGISTER_RUNNING_DATA_SIZE
    value = int.from_bytes(buffer[byte_offset:byte_offset + REGISTER_RUNNING_DATA_SIZE], byteorder="big", signed=False)
    return float(value) / gain if value != 0xffff else 0
  
  def read_float_4(self, buffer: bytes, gain: int, register_offset: int) -> float:
    """Retrieve value (4 unsigned bytes) from buffer"""
    byte_offset = register_offset * REGISTER_RUNNING_DATA_SIZE
    value = int.from_bytes(buffer[byte_offset:byte_offset + (REGISTER_RUNNING_DATA_SIZE * 2)], byteorder="big", signed=False)
    return float(value) / gain if value != 0xffff else 0
  
  def update(self, data: bytes):
    self.running_info.vpv1 = self.read_float(data, 10, R_VPV1 - REGISTER_RUNNING_DATA)
    self.running_info.ipv1 = self.read_float(data, 10, R_IPV1 - REGISTER_RUNNING_DATA)
    self.running_info.ppv1 = self.running_info.vpv1 * self.running_info.ipv1
    self.running_info.vac1 = self.read_float(data, 10, R_VAC1 - REGISTER_RUNNING_DATA)
    self.running_info.iac1 = self.read_float(data, 10, R_IAC1 - REGISTER_RUNNING_DATA)
    self.running_info.pac = self.running_info.vac1 * self.running_info.iac1
    self.running_info.fac1 = self.read_float(data, 100, R_FAC1 - REGISTER_RUNNING_DATA)
    self.running_info.workMode = self.read_int(data, R_WORK_MODE - REGISTER_RUNNING_DATA)
    self.running_info.temp = self.read_float(data, 10, R_TEMP - REGISTER_RUNNING_DATA)
    self.running_info.eTotal = self.read_float_4(data, 10, R_TOTAL_FEED_POWER - REGISTER_RUNNING_DATA)
    self.running_info.eDay = self.read_float(data, 10, R_TODAY_FEED_POWER - REGISTER_RUNNING_DATA)


class State(IntEnum):
  DISCONNECTED = 1
  CONNECTED = 2
  ONLINE = 3

# MODBUS COMMANDS
MODBUS_READ_CMD: int = 0x3
MODBUS_WRITE_CMD: int = 0x6
MODBUS_WRITE_MULTI_CMD: int = 0x10

# MODBUS BYTES LAYOUT
MODBUS_ADDRESS_INDEX = 0
MODBUS_COMMAND_INDEX = 1
MODBUS_FAILURE_INDEX= 2
MODBUS_PAYLOAD_LENGTH_INDEX = 2
MODBUS_PAYLOAD_START = 3
MODBUS_CRC_START = -2

# REGISTERS
REGISTER_SERIAL_NUMBER: int = 512
REGISTER_RUNNING_DATA: int = 768

REGISTER_RUNNING_DATA_SIZE: int = 2

R_VPV1 = 768
R_VPV2 = 769
R_IPV1 = 770
R_IPV2 = 771
R_VAC1 = 772
R_VAC2 = 773
R_VAC3 = 774
R_IAC1 = 775
R_IAC2 = 776
R_IAC3 = 777
R_FAC1 = 778
R_FAC2 = 779
R_FAC3 = 780
R_WORK_MODE = 782
R_TEMP = 783
R_ERROR = 784
R_TOTAL_FEED_POWER = 786
R_TOTAL_FEED_HOURS = 788
R_FIRMWARE = 790
R_WARNING = 791
R_FUNCBIT = 793
R_TODAY_FEED_POWER = 800


class GoodWeCommunicator(object):
  DEFAULT_RESETWAIT = 5          # default wait time in seconds

  def __init__(self, serialPort: str):
    self.serial_port = serialPort
    self.last_received = 0       # timeout detection
    self.state = State.DISCONNECTED
    self.statetime = millis()
    self.inverter = Inverter()
    self.device = None
    logging.info("Goodwe initialized")

  def reset_tty_device(self):
    logging.debug("Going to reset tty device at")
    self.close_device()
    time.sleep(self.DEFAULT_RESETWAIT)

    self.last_received = 0
    self.inverter.running_info = RunningInfo()
    if self.open_device():
      self.set_state(State.CONNECTED)

  def open_device(self):
    logging.debug("Going to open serial device at %s", self.serial_port)
    try:
      self.device = serial.Serial(port=self.serial_port, baudrate=9600, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE, bytesize=serial.EIGHTBITS, timeout=2)
      logging.info("Serial device opened at %s", self.serial_port)

      return True
    except Exception:
      logging.exception("Unable to open serial device")
      logging.error("Failed to open serial device at %s", self.serial_port)
      return False

  def close_device(self):
    logging.debug("Going to close serial device")
    if self.device is not None:
      try:
        self.device.close()
        self.device = None
        logging.info("Serial device closed successfully")
      except Exception:
        logging.exception("Unable to close serial device")
        logging.error("Failed to close serial device")

  def set_state(self, state):
    logging.debug("Going change state to: %d", state)
    self.state = state
    self.statetime = millis()
  
  def validate_response(self, data: bytes, command: int, length: int) -> bool:
    """
    Validate the modbus RTU response.
    data[0]    is inverter address
    data[1]    is command return type
    data[2]    is response payload length (for read commands)
    data[3:-2] is the the response payload
    data[-2:]  is crc-16 checksum
    """
    if len(data) < 5:
        logging.debug("Response is too short.")
        raise PartialResponseException(len(data), 5)
    if data[MODBUS_COMMAND_INDEX] == MODBUS_READ_CMD:
        if data[MODBUS_PAYLOAD_LENGTH_INDEX] != length * 2:
            logging.debug("Response has unexpected length: %d, expected %d.", data[MODBUS_PAYLOAD_LENGTH_INDEX], length * 2)
            raise PartialResponseException(len(data), length * 2)
        expected_length = data[MODBUS_PAYLOAD_LENGTH_INDEX] + 5
        if len(data) < expected_length:
            raise PartialResponseException(len(data), expected_length)
    else:
        expected_length = len(data)

    checksum_offset = expected_length - 2
    if modbus_checksum(data[:checksum_offset]) != ((data[checksum_offset + 1] << 8) + data[checksum_offset]):
        logging.debug("Response CRC-16 checksum does not match.")
        raise RequestFailedException("CRC-16 does not match")

    if data[MODBUS_COMMAND_INDEX] != command:
        failure_code = FAILURE_CODES.get(data[MODBUS_FAILURE_INDEX], "UNKNOWN")
        logging.debug("Response is command failure: %s.", FAILURE_CODES.get(data[MODBUS_FAILURE_INDEX], "UNKNOWN"))
        raise RequestRejectedException(failure_code)

  def send_request(self, command: int, register: int, length: int) -> None:
    """
    Create modbus RTU request.
    data[0] is inverter address
    data[1] is modbus command
    data[2:3] is command offset parameter
    data[4:5] is command value parameter
    data[6:7] is crc-16 checksum
    """
    data: bytearray = bytearray(6)
    data[MODBUS_ADDRESS_INDEX] = self.inverter.modbus_address
    data[MODBUS_COMMAND_INDEX] = command
    data[2] = (register >> 8) & 0xFF
    data[3] = register & 0xFF
    data[4] = (length >> 8) & 0xFF
    data[5] = length & 0xFF
    checksum = modbus_checksum(data)
    data.append(checksum & 0xFF)
    data.append((checksum >> 8) & 0xFF)
  
    try:
      self.device.write(data)
    except serial.SerialException:
      logging.exception("Unable to write to serial device")
      self.set_state(State.DISCONNECTED)
      raise InverterError("Connection lost")

  def read_response(self, command: int, length: int) -> bytes:
    try:
      response = self.device.read_all()
      if (response == None or len(response) == 0):
        logging.debug("No response from device, going to rediscover.")
        self.inverter.is_online = False
        self.set_state(State.CONNECTED)
        raise RequestFailedException("Unexpected empty response")

      self.last_received = millis()
      self.inverter.is_online = True
      logging.debug("Received response: %s", response.hex())
      self.validate_response(response, command, length)
      return response[MODBUS_PAYLOAD_START:MODBUS_CRC_START]
    except serial.SerialException:
      logging.exception("Unable to read from serial device")
      self.set_state(State.DISCONNECTED)
      raise InverterError("Connection lost")

  def send_discovery(self):
    logging.debug("Going to send discovery")
    self.running_info = RunningInfo()
    self.send_request(MODBUS_READ_CMD, REGISTER_SERIAL_NUMBER, 8)
    time.sleep(3)
    response = self.read_response(MODBUS_READ_CMD, 8)
    self.inverter.serial = response.decode("ascii")
    logging.info("Discovered inverter: %s", self.inverter.serial)
    self.set_state(State.ONLINE)

  def update_inverter_data(self):
    logging.debug("Going ask inverter for information")
    self.send_request(MODBUS_READ_CMD, REGISTER_RUNNING_DATA, 85)
    time.sleep(3)
    response = self.read_response(MODBUS_READ_CMD, 85)
    self.inverter.update(response)

  def get_inverter(self):
    return self.inverter

  def handle(self):
    try:
      match self.state:
        case State.DISCONNECTED:
          self.reset_tty_device()
        case State.CONNECTED:
          self.send_discovery()
        case State.ONLINE:
          self.update_inverter_data()
    except InverterError:
      logging.exception("Received an error")

