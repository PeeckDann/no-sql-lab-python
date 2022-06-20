import os
import redis

COMPLETED = "COMPLETED"
IN_PROGRESS = "IN_PROGRESS"


class Redis:
    def __init__(self, identifier, strategy):
        host = os.getenv("REDIS__HOST")
        port = os.getenv("REDIS__PORT", 6380)
        password = os.getenv("REDIS__PASSWORD")
        self.redis_client = redis.StrictRedis(host=host, port=port, password=password, ssl=True)
        self.status_key = f"{identifier}_{strategy}_event_hub_status"
        self.indexes_key = f"{identifier}_{strategy}_event_hub_indexes"

    def was_data_processed(self):
        redis_status = self.redis_client.get(self.status_key)
        if not redis_status:
            return False

        status = redis_status.decode()
        if status != COMPLETED:
            return False
        return True

    def was_chunk_processed(self, offset):
        processed_batches = set(int(index) for index in self.redis_client.lrange(self.indexes_key, 0, -1))
        if offset in processed_batches:
            return True
        return False

    def set_in_progress_status(self):
        self.redis_client.set(self.status_key, IN_PROGRESS)

    def set_intermediate_status(self, offset):
        self.redis_client.lpush(self.indexes_key, offset)

    def _set_completed_status(self):
        self.redis_client.set(self.status_key, COMPLETED)

    def finish_writing(self):
        self.redis_client.delete(self.indexes_key)
        self._set_completed_status()
