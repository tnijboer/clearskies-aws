from clearskies.backends.backend import Backend
from clearskies import model
import json
from typing import Any, Callable, Dict, List, Tuple
class SqsBackend(Backend):
    """
    SQS backend for clearskies

    There's not too much to this.  Just set it on your model and set the table name equal to the SQS url.

    This doesn't support setting message attributes.  The SQS call is simple enough that if you need
    those you may as well just invoke the boto3 SDK yourself.

    Note that this is a *write-only* backend.  Reading from an SQS queue is different enough from
    the way that clearskies models works that it doesn't make sense to try to make those happen here.

    See the SQS context in this library for processing your queue data.
    """

    _boto3 = None
    _environment = None
    _sqs = None

    _allowed_configs = [
        'table_name',
        'model_columns',
    ]

    _required_configs = [
        'table_name',
    ]

    def __init__(self, boto3, environment):
        self._boto3 = boto3
        self._environment = environment
        if not environment.get('AWS_REGION', True):
            raise ValueError('To use SQS you must use set AWS_REGION in the .env file or an environment variable')

        self._sqs = self._boto3.client('sqs', region_name=environment.get('AWS_REGION', True))

    def configure(self):
        pass

    def create(self, data, model):
        self._sqs.send_message(
            QueueUrl=model.table_name(),
            MessageBody=json.dumps(data),
        )
        return {**data}

    def update(self, id, data, model):
        raise ValueError("The SQS backend only supports the create operation")

    def delete(self, id, model):
        raise ValueError("The SQS backend only supports the create operation")

    def count(self, configuration, model):
        raise ValueError("The SQS backend only supports the create operation")

    def records(self,
                configuration: Dict[str, Any],
                model: model.Model,
                next_page_data: Dict[str, str] = None) -> List[Dict[str, Any]]:
        raise ValueError("The SQS backend only supports the create operation")
        return []

    def validate_pagination_kwargs(self, kwargs: Dict[str, Any], case_mapping: Callable) -> str:
        return ''

    def allowed_pagination_keys(self) -> List[str]:
        return []

    def documentation_pagination_next_page_response(self, case_mapping: Callable) -> List[Any]:
        return []

    def documentation_pagination_next_page_example(self, case_mapping: Callable) -> Dict[str, Any]:
        return {}

    def documentation_pagination_parameters(self, case_mapping: Callable) -> List[Tuple[Any]]:
        return []
