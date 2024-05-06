import logging
from opcua import Client
from azure.iot.device import IoTHubDeviceClient, Message
import os
import json
import datetime
import uuid
import time
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import json
import uuid

load_dotenv()
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
print(CONNECTION_STRING)


# MQTT server details
SERVER = "172.23.83.254"
PORT = 1883
TOPIC = "Advantech/+/data"

# Callback function on connection
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(TOPIC)

# Create an instance of the device client using the connection string
iot_hub_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

# Modify the on_message function to send the message to IoT Hub
def on_message(client, userdata, msg):
    print(f"Received message '{msg.payload.decode()}' on topic '{msg.topic}'")
    
    # Load the payload
    sensor_data = json.loads(msg.payload.decode())
    
    # Check if the payload is a dictionary
    if isinstance(sensor_data, dict):
        # Convert the sensor data to the expected format
        formatted_sensor_data = {}
        for sensor_id, reading in sensor_data.items():
            
            # Check if the reading is numeric
            if isinstance(reading, (int, float)):
                formatted_sensor_data[sensor_id] = {
                    "value": reading,  # The sensor reading
                    "unit": "unit"  # Replace with the actual unit of the sensor reading
                }
        
        # Only send a message if there is at least one sensor reading
        if formatted_sensor_data:
            # Create a Message object from the payload
            payload = {
                "Device": "Wise",  # Replace with your device name
                "Timestamp": datetime.datetime.now().isoformat(),  # Current timestamp
                "Data": formatted_sensor_data
            }
            
            # Convert the payload to a JSON string
            payload_str = json.dumps(payload)
            
            # Create a Message object from the payload string
            message = Message(payload_str)
            
            # Send the message to IoT Hub
            iot_hub_client.send_message(message)
            print("Message sent to IoT Hub")

# Create an MQTT client and attach our routines to it
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(SERVER, PORT, 60)

# Process network traffic and dispatch callbacks
mqtt_client.loop_forever()