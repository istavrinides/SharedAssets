import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from random import randrange, random, choice
from datetime import datetime
import json
import time

tenant = '1'
producer = EventHubProducerClient.from_connection_string(conn_str="<Your connection string>", eventhub_name="<Your Event Hub Name>")

async def run():
    while True:
        try:
            msg = {}
            is_error = choice([True, False])
            device_id = 1
            temperature = randrange(30, 60) + random()
            pressure = randrange(100, 150) + random()
            ts = datetime.now().isoformat()
            msg["deviceId"] = "tenant" + tenant + "-" + str(device_id)
            msg["temperature"] = temperature
            msg["pressure"] = pressure
            msg["ts"] = ts
            msg["source"] = "eventhub"

            print(json.dumps(msg))

            async with producer:
                # Create a batch.
                event_data_batch = await producer.create_batch(partition_id = '0')

                # Add events to the batch.
                event_data_batch.add(EventData(json.dumps(msg)))

                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)

            time.sleep(0.01)
        except Exception as e:
            print(e) 

loop = asyncio.get_event_loop()
loop.run_until_complete(run())