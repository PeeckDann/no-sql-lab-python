import json

from .writer_interface import WriterInterface


class ConsoleWriter(WriterInterface):
    def __init__(self, *args, **kwargs):
        pass

    def write_data(self, content, offset):
        print(json.dumps(content))
