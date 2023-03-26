import json
import unittest
from unittest.mock import MagicMock, call
from types import SimpleNamespace
from .lambda_sqs_standard_partial_batch import lambda_sqs_standard_partial_batch
class LambdaSqsStandardPartialBatchTest(unittest.TestCase):
    def setUp(self):
        self.calls = []

    def my_callable(self, request_data):
        if 'boom' in request_data:
            raise ValueError('oops')
        self.calls.append(request_data)

    def test_simple_execution(self):
        sqs_handler = lambda_sqs_standard_partial_batch(self.my_callable)
        sqs_handler(
            {
                'Records': [
                    {
                        'messageId': '1-2-3-4',
                        'body': json.dumps({'hey': 'sup'}),
                    },
                    {
                        'messageId': '2-3-4-5',
                        'body': json.dumps({'cool': 'yo'}),
                    },
                ]
            },
            {},
        )
        self.assertEquals(
            [
                {
                    'hey': 'sup'
                },
                {
                    'cool': 'yo'
                },
            ],
            self.calls,
        )

    def test_with_failure(self):
        sqs_handler = lambda_sqs_standard_partial_batch(self.my_callable)
        results = sqs_handler({
            'Records': [
                {
                    'messageId': '1-2-3-4',
                    'body': json.dumps({'hey': 'sup'}),
                },
                {
                    'messageId': '2-3-4-5',
                    'body': json.dumps({'boom': 'yo'}),
                },
            ]
        }, {})
        self.assertEquals(
            [
                {
                    'hey': 'sup'
                },
            ],
            self.calls,
        )
        self.assertEquals(
            {'batchItemFailures': [{
                'itemIdentifier': '2-3-4-5'
            }]},
            results,
        )
