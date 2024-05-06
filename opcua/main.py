import logging
from opcua import Client
from azure.iot.device import IoTHubDeviceClient, Message
import os
import json
import datetime
import uuid
import time
from dotenv import load_dotenv

load_dotenv()

# Constants
DELAY = 5
NODES_TO_READ = [
    "ns=4;s=Root.outputs.Digital_Outputs_0",
    "ns=4;s=Root.heating.temperature_reactor",
    "ns=4;s=Root.outputs.Digital_Outputs_1",
    "ns=4;s=Root.inputs.Digital_Inputs_0",
    "ns=4;s=Root.inputs.Digital_Inputs_1",
    "ns=4;s=Root.heating.setpoint_heating",
    "ns=4;s=Root.setter/resetter.set_output4",
    "ns=4;s=Root.setter/resetter.switch_output5",
    "ns=4;s=Root.setter/resetter.reset_output4",
    "ns=4;s=Root.setter/resetter.switch_output3"
]

# Get environment variables
OPCUA_SERVER_URL = os.getenv('OPCUA_SERVER_URL')
CONNECTION_STRING = os.getenv('CONNECTION_STRING')
SLEEP_TIME =  20

# Set a flag to indicate that the socket is open
socket_is_open = True

# Create an empty dictionary to store the data
data = {}

try:
    # Create an Azure IoT Hub client
    azure_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
except Exception as e:
    print("Error creating Azure IoT Hub client: %s", e)
    exit(1)

try:
    # Create an OPC UA client and connect to the server
    opcua_client = Client(OPCUA_SERVER_URL)
    opcua_client.connect()
except Exception as e:
    print("Error creating OPC UA client: %s", e)
    exit(1)

try:
    # Create an Azure IoT Hub client
    azure_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

    # Use a context manager to handle the connection to the OPC UA server
    with opcua_client:
        while True:  # Add this line to start the loop
            # Read data from OPC UA server and send it to Azure IoT Hub
            sending_json = {}
            data = {}
            for node_path in NODES_TO_READ:
                # Haal het knooppunt op
                var = opcua_client.get_node(node_path)

                try:
                    value = var.get_value()
                    data[node_path.split(";")[-1]] = value
                except Exception as e:
                    print("Error reading value for node", node_path, ":", e)

            # Voeg timestamp en unieke UID toe aan de data

            timestamp = datetime.datetime.utcnow().isoformat()
            sending_json["Device"] = "OPCUA"
            sending_json["Timestamp"] = timestamp
            sending_json["Data"] = data

            # Convert data to JSON string
            payload = json.dumps(sending_json)

            # Create message and send to output1
            message = Message(payload)
            print("Sending message: %s", message)
            try:
                azure_client.send_message(message)
            except Exception as e:
                print("Error sending message: %s", e)

            time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
    print("Exiting...")

finally:
    print("Disconnecting from Azure IoT Hub...")
    azure_client.disconnect()
    opcua_client.disconnect()
    print("Disconnected.")
    print("Exiting.")