import os
import json
import redis

from azure.eventhub import EventHubProducerClient, EventData
from .writer_interface import WriterInterface

COMPLETED = "COMPLETED"
IN_PROGRESS = "IN_PROGRESS"


class EventHubWriter(WriterInterface):
    def __init__(self, identifier):
        # Redis init
        host = os.getenv("REDIS__HOST")
        port = os.getenv("REDIS__PORT", 6380)
        password = os.getenv("REDIS__PASSWORD")
        self.redis_client = redis.StrictRedis(host=host, port=port, password=password, ssl=True)

        # Event hub init
        conn_str = os.getenv("EVENT_HUB__CONNECTION")
        eventhub_name = os.getenv("EVENT_HUB__NAME")
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=eventhub_name
        )

        # Redis status keys
        self.status_key = f"{identifier}_event_hub_status"
        self.indexes_key = f"{identifier}_event_hub_indexes"

    def write_data(self, content, offset):
        if self._was_chunk_processed(offset):
            return

        event_data_batch = self.producer.create_batch()
        for item in content:
            event_data_batch.add(EventData(json.dumps(item)))
        self.producer.send_batch(event_data_batch)

        self._set_intermediate_status(offset)

    def was_data_processed(self):
        redis_status = self.redis_client.get(self.status_key)
        if not redis_status:
            return False

        status = redis_status.decode()
        if status != COMPLETED:
            return False
        return True

    def _was_chunk_processed(self, offset):
        processed_batches = set(int(index) for index in self.redis_client.lrange(self.indexes_key, 0, -1))
        if offset in processed_batches:
            return True
        return False

    def set_in_progress_status(self):
        self.redis_client.set(self.status_key, IN_PROGRESS)

    def _set_intermediate_status(self, offset):
        self.redis_client.lpush(self.indexes_key, offset)

    def _set_completed_status(self):
        self.redis_client.set(self.status_key, COMPLETED)

    def finish_writing(self):
        self.redis_client.delete(self.indexes_key)
        self._set_completed_status()
        self.producer.close()
