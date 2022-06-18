class WriterInterface:
    def write_data(self, content, offset):
        pass

    def was_data_processed(self):
        pass

    def set_in_progress_status(self):
        pass

    def finish(self):
        pass
