from ...actions.ses import SES as BaseSES
class SES(BaseSES):
    calls = None

    def __init__(self, environment, boto3, di):
        super().__init__(environment, boto3, di)

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSES, SES)

    def __call__(self, model) -> None:
        """Send a notification as configured."""
        if SES.calls == None:
            SES.calls = []
        utcnow = self.di.build('utcnow')
        if self.when and not self.di.call_function(self.when, model=model):
            return

        SES.calls.append({
            "from": self.sender,
            "to": self._resolve_destination("to", model),
            "cc": self._resolve_destination("cc", model),
            "bcc": self._resolve_destination("bcc", model),
            "subject": self._resolve_subject(model, utcnow),
            "message": self._resolve_message_as_html(model, utcnow),
        })
