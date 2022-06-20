import os
import json

from azure.eventhub import EventHubProducerClient, EventData
from .writer_interface import WriterInterface


class EventHubWriter(WriterInterface):
    def __init__(self):
        conn_str = os.getenv("EVENT_HUB__CONNECTION")
        eventhub_name = os.getenv("EVENT_HUB__NAME")
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=eventhub_name
        )

    def write_data(self, content, offset):
        event_data_batch = self.producer.create_batch()
        for item in content:
            event_data_batch.add(EventData(json.dumps(item)))
        self.producer.send_batch(event_data_batch)

    def finish_writing(self):
        self.producer.close()
