import os
import asyncio

from urllib.parse import urlparse
from sodapy import Socrata

from utils.utils import get_data_chunks
from .writers.console_writer import ConsoleWriter
from .writers.event_hub_writer import EventHubWriter

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
        print(self.url, self.identifier)
        self.writer = STRATEGIES[self.output_strategy](self.identifier)

    def process_data(self):
        if self.writer.was_data_processed():
            return "Already wrote this data to {}".format(self.output_strategy)
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._fetch_data())
        result = loop.run_until_complete(self._upload_data(data))
        return result

    async def _upload_data(self, content):
        try:
            self.writer.set_in_progress_status()
            for data_chunk, offset in get_data_chunks(content, self.data_chunk_size):
                self.writer.write_data(data_chunk, offset)

            self.writer.finish()
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
