import unittest
from unittest.mock import MagicMock, call
from .ses import SES
import clearskies
from ..di import StandardDependencies
class SESTest(unittest.TestCase):
    def setUp(self):
        self.di = StandardDependencies()
        self.di.bind('environment', {'AWS_REGION': 'us-east-2'})
        self.ses = MagicMock()
        self.ses.send_email = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.ses)
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value='us-east-1')

    def test_send(self):
        ses = SES(self.environment, self.boto3, self.di)
        ses.configure(
            'test@example.com', to='jane@example.com', subject='welcome!', message_template='hi {{ model.id }}!'
        )
        model = MagicMock()
        model.id = 'asdf'
        ses(model)
        self.ses.send_email.assert_has_calls([
            call(
                Destination={
                    'ToAddresses': ['jane@example.com'],
                    'CcAddresses': [],
                    'BccAddresses': []
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': 'utf-8',
                            'Data': 'hi asdf!'
                        },
                        'Text': {
                            'Charset': 'utf-8',
                            'Data': 'hi asdf!'
                        }
                    },
                    'Subject': {
                        'Charset': 'utf-8',
                        'Data': 'welcome!'
                    }
                },
                Source='test@example.com'
            ),
        ])

    def test_send_callable(self):
        ses = SES(self.environment, self.boto3, self.di)
        ses.configure(
            'test@example.com',
            to=lambda model: 'jane@example.com',
            bcc=lambda model: ['bob@example.com', 'greg@example.com'],
            subject='welcome!',
            message_template='hi {{ model.id }}!'
        )
        model = MagicMock()
        model.id = 'asdf'
        ses(model)
        self.ses.send_email.assert_has_calls([
            call(
                Destination={
                    'ToAddresses': ['jane@example.com'],
                    'CcAddresses': [],
                    'BccAddresses': ['bob@example.com', 'greg@example.com']
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': 'utf-8',
                            'Data': 'hi asdf!'
                        },
                        'Text': {
                            'Charset': 'utf-8',
                            'Data': 'hi asdf!'
                        }
                    },
                    'Subject': {
                        'Charset': 'utf-8',
                        'Data': 'welcome!'
                    }
                },
                Source='test@example.com'
            ),
        ])
