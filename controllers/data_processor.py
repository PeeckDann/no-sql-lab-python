import os
import asyncio

from urllib.parse import urlparse
from sodapy import Socrata

from utils.utils import get_data_chunks
from .writers.console_writer import ConsoleWriter
from .writers.event_hub_writer import EventHubWriter
from .redis.redis import Redis

OUTPUT_STRATEGY = os.getenv("OUTPUT__STRATEGY", "console").lower()
DATA_CHUNK_SIZE = int(os.getenv("DATA_CHUNK_SIZE", 200))

STRATEGIES = {
    "console": ConsoleWriter,
    "event_hub": EventHubWriter,
}


class DataProcessor:
    def __init__(self, strategy, dataset_endpoint):
        self.output_strategy = strategy if (strategy and strategy in list(STRATEGIES)) \
            else OUTPUT_STRATEGY
        self.data_chunk_size = DATA_CHUNK_SIZE
        self.url, self.identifier = DataProcessor._parse_endpoint(dataset_endpoint)
        self.writer = STRATEGIES[self.output_strategy]()
        self.redis = Redis(self.identifier, self.output_strategy)

    def process_data(self):
        if self.redis.was_data_processed():
            return "Already wrote this data to {}".format(self.output_strategy)
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._fetch_data())
        result = loop.run_until_complete(self._upload_data(data))
        return result

    async def _upload_data(self, content):
        try:
            self.redis.set_in_progress_status()
            for data_chunk, offset in get_data_chunks(content, self.data_chunk_size):
                if self.redis.was_chunk_processed(offset):
                    continue
                self.writer.write_data(data_chunk, offset)
                self.redis.set_intermediate_status(offset)

            self.writer.finish_writing()
            self.redis.finish_writing()
        except Exception as e:
            raise ProcessingError("Couldn't write to {}. Error: {}".format(
                self.output_strategy, str(e)
            ))
        return "Successfully written to {}".format(self.output_strategy)

    async def _fetch_data(self):
        with Socrata(self.url, None) as client:
            return client.get_all(self.identifier)

    @staticmethod
    def _parse_endpoint(endpoint):
        url = urlparse(endpoint).netloc
        identifier = os.path.basename(endpoint).split(".")[0]
        return url, identifier


class ProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg
