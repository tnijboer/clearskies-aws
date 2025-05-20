import unittest
from collections import OrderedDict

from .lambda_api_gateway import LambdaAPIGateway


class LambdaAPIGatewayTest(unittest.TestCase):
    dummy_event = {
        'httpMethod': 'GET',
        'path': '/test',
        'resource': 'bob',
        'queryStringParameters': {
            'q': 'hey',
            'bob': 'sup'
        },
        'pathParameters': None,
        'headers': {
            'Content-Type': 'application/json'
        },
    }

    def test_respond(self):
        aws_lambda = LambdaAPIGateway(self.dummy_event, {})
        aws_lambda.set_headers({'bob': 'hey', 'jane': 'kay'})
        aws_lambda.set_header('hey', 'sup')
        aws_lambda.clear_header('bob')
        response = aws_lambda.respond({'some': 'data'}, 200)
        self.assertEqual(
            {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": OrderedDict(
                    [
                        ("JANE", "kay"),
                        ("HEY", "sup"),
                        ("CONTENT-TYPE", "application/json; charset=UTF-8"),
                    ]
                ),
                "body": '{"some": "data"}',
            },
            response,
        )

    def test_headers(self):
        aws_lambda = LambdaAPIGateway({
            **self.dummy_event,
            **{
                'headers': {
                    'Content-Type': 'application/json',
                    'AUTHORIZATION': 'hey',
                    'X-Auth': 'asdf',
                }
            }
        }, {})
        self.assertEqual("hey", aws_lambda.get_request_header("authorizatiON"))
        self.assertEqual("asdf", aws_lambda.get_request_header("x-auth"))
        self.assertTrue(aws_lambda.has_request_header('authorization'))
        self.assertTrue(aws_lambda.has_request_header('x-auth'))
        self.assertFalse(aws_lambda.has_request_header('bearer'))

    def test_body_plain(self):
        aws_lambda = LambdaAPIGateway({**self.dummy_event, **{'body': '{"hey": "sup"}', 'isBase64Encoded': False}}, {})

        self.assertEqual({"hey": "sup"}, aws_lambda.json_body())
        self.assertEqual('{"hey": "sup"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_body_base64(self):
        aws_lambda = LambdaAPIGateway({
            **self.dummy_event,
            **{
                'body': 'eyJoZXkiOiAic3VwIn0=',
                'isBase64Encoded': True
            }
        }, {})

        self.assertEqual({"hey": "sup"}, aws_lambda.json_body())
        self.assertEqual('{"hey": "sup"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_path(self):
        aws_lambda = LambdaAPIGateway(self.dummy_event, {})
        self.assertEqual("/test", aws_lambda.get_path_info())

    def test_query_string(self):
        aws_lambda = LambdaAPIGateway(self.dummy_event, {})
        self.assertEqual("q=hey&bob=sup", aws_lambda.get_query_string())
