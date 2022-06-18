from flask import request, Blueprint

from controllers.data_processor import DataProcessor, ProcessingError

load = Blueprint("load", __name__)


@load.route("/data/load")
def load_file():
    dataset_endpoint = request.args.get("dataset_endpoint")
    strategy = request.args.get("strategy")
    if not dataset_endpoint:
        return "No dataset endpoint provided!", 400

    data_processor = DataProcessor(strategy, dataset_endpoint)
    try:
        result = data_processor.process_data()
    except ProcessingError as e:
        return e.msg, 500

    return result
