import threading
from opcua import Client
import os
from dotenv import load_dotenv

load_dotenv('.env')

# Get environment variables
OPCUA_SERVER_URL = os.getenv('opc.tcp://172.23.177.83:48020')

def opcua_connect():
    try:
        # Create an OPC UA client and connect to the server
        opcua_client = Client(OPCUA_SERVER_URL)
        opcua_client.connect()
        print("Connected to OPC UA server.")
    except Exception as e:
        print("Error creating OPC UA client: %s", e)

# Create and start the OPC UA threads in a loop
try:
    while True:
        opcua_thread = threading.Thread(target=opcua_connect)
        opcua_thread.daemon = True
        opcua_thread.start()

except KeyboardInterrupt:
    print("Exiting...")
    exit(0)