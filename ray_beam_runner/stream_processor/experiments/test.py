import ray
from apache_beam.runners.portability.fn_api_runner.translations import Stage
from cloudpickle import Pickler

from stream_processor.stage_actor import SDKProcess

Pickler.dump(SDKProcess.remote(True, Stage()))
#Test