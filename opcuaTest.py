from opcua import Client
from azure.iot.device import IoTHubDeviceClient, Message
import json
import uuid
import datetime
import time

# Create an empty dictionary to store the data
data = {}

# OPC UA server details
opcua_server_url = "opc.tcp://172.23.177.83:48020"
nodes_to_read = [
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

# Azure IoT Hub details
connection_string = "HostName=iothubavb.azure-devices.net;DeviceId=arnopi;SharedAccessKey=EwKYElUBpLZg3p9LgrDdi3UoMzqV7MkrpAIoTKX+nkg="

# Create an OPC UA client and connect to the server
opcua_client = Client(opcua_server_url)
opcua_client.connect()

# Create an Azure IoT Hub client
azure_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

# Read data from OPC UA server and send it to Azure IoT Hub
data = {}
for node_path in nodes_to_read:
    # Haal het knooppunt op
    var = opcua_client.get_node(node_path)

    try:
        value = var.get_value()
        data[node_path.split(";")[-1]] = value
    except Exception as e:
        print("Error reading value for node", node_path, ":", e)

# Voeg timestamp en unieke UID toe aan de data
timestamp = datetime.datetime.utcnow().isoformat()
uid = str(uuid.uuid4())
data["timestamp"] = timestamp
data["uid"] = uid

# Convert data to JSON string
payload = json.dumps(data)

# Create message and send to output1
message = Message(payload)
print("Sending message: {}".format(message))
azure_client.send_message(message)

# Pauze van 1 seconde tussen elke iteratie
time.sleep(300)