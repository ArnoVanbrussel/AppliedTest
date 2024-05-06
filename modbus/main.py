import sys
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
from pyModbusTCP.client import ModbusClient
import uuid
import time
import json
import datetime
from dotenv import load_dotenv
from azure.iot.device import IoTHubDeviceClient, Message
import os


load_dotenv()


CONNECTION_STRING = os.getenv('CONNECTION_STRING')
SLEEP_TIME =  10


# Create an empty dictionary to store the data
data = {}

#Create an Azure IoT Hub client
azure_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

    
ModBusClient = ModbusClient(host='172.23.176.39', unit_id=1, port=502, auto_open=True, auto_close=True)
sensors = ['Brightness', 'Windspeed', 'Temperature', 'Humidity']

try:

        while True:  # Add this line to start the loop
            # Read data from OPC UA server and send it to Azure IoT Hub
            regs = ModBusClient.read_holding_registers(4, 4)
            sending_json = {}

            data = {}

            for i in range(4):
                # Convert the 16-bit register value to a voltage (-10V to +10V)
                voltage = (regs[i] / 32768 - 1) * 10
                if voltage < 0:
                    voltage = 0
                else:
                    # Convert the voltage to a sensor value
                    if sensors[i] == 'Temperature':
                        # Temperature: -20°C at 0V, +60°C at 10V
                        value = round(voltage * 8 - 20, 2)
                        unit = "°C"
                    elif sensors[i] == 'Humidity':
                        # Humidity: 0% at 0V, 100% at 10V
                        value = round(voltage * 10, 2)
                        unit = "% rel. h"
                    elif sensors[i] == 'Windspeed':
                        # Windspeed: 0 m/s at 0V, 40 m/s at 10V
                        value = round(voltage * 4, 2)
                        unit = "m/s"
                    elif sensors[i] == 'Brightness':
                        # Brightness: 0 kLx at 0V, 150 kLx at 10V
                        value = round(voltage * 15, 2)
                        unit = "kLx"

                    data[sensors[i]] = {"value": value, "unit": unit}

            # Add timestamp and unique UID to data
            timestamp = datetime.datetime.utcnow().isoformat()
            uid = str(uuid.uuid4())
            sending_json["Device"] = "Modbus"
            sending_json["Timestamp"] = timestamp
            sending_json["Data"] = data


            # Convert data to JSON string
            payload = json.dumps(sending_json)

            message = Message(payload)
            print("Sending message: {}".format(message))
            try:
                azure_client.send_message(message)
            except Exception as e:
                print("Error sending message: %s", e)
            time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
    print("Keyboard interupts...")

finally:
    print("Disconnecting from Azure IoT Hub...")
    azure_client.disconnect()
    ModBusClient.close()
    print("Disconnected.")
    print("Exiting.")
    sys.exit()