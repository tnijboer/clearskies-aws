from typing import List
import datetime
import clearskies
from botocore.exceptions import ClientError
from collections.abc import Sequence
class SES:
    def __init__(self, boto3, di):
        self.boto3 = boto3
        self.di = di

    def configure(
        self,
        sender,
        to=None,
        cc=None,
        bcc=None,
        subject=None,
        message=None,
        subject_template=None,
        message_template=None,
        subject_template_file=None,
        message_template_file=None,
        assume_role=None,
        dependencies_for_template=None,
        when=None,
    ) -> None:
        """Configure the rules for this email notification."""
        self.destinations = {
            "to": [],
            "cc": [],
            "bcc": [],
        }
        # this just moves the data from the various "to" inputs (to, cc, bcc) into the self.destinations
        # dictionary, after normalizing it so that it is always a list.
        for key in self.destinations.keys():
            destination_values = locals()[key]
            if not destination_values:
                continue
            if type(destination_values) == str:
                self.destinations[key] = [destination_values]
            else:
                self.destinations[key] = destination_values
        self.subject = subject
        self.message = message
        self.sender = sender
        self.subject_template = None
        self.message_template = None
        self.dependencies_for_template = dependencies_for_template if dependencies_for_template else []
        if when is not None and not callable(when):
            raise ValueError("'when' must be a callable but it was something else")
        self.when = when

        if not to and not cc:
            raise ValueError("You must configure at least one 'to' address or one 'cc' address")
        num_subjects = 0
        num_messages = 0
        for source in [subject, subject_template, subject_template_file]:
            if source:
                num_subjects += 1
        for source in [message, message_template, message_template_file]:
            if source:
                num_messages += 1
        if num_subjects > 1:
            raise ValueError(
                "More than one of 'subject', 'subject_template', or 'subject_template_file' was set, but only one of these may be set."
            )
        if num_messages > 1:
            raise ValueError(
                "More than one of 'message', 'message_template', or 'message_template_file' was set, but only one of these may be set."
            )

        if subject_template_file:
            import jinja2
            with open(subject_template_file, "r", encoding="utf-8") as template:
                self.subject_template = jinja2.Template(template.read())
        elif subject_template:
            import jinja2
            self.subject_template = jinja2.Template(subject_template)

        if message_template_file:
            import jinja2
            with open(message_template_file, "r", encoding="utf-8") as template:
                self.message_template = jinja2.Template(template.read())
        elif message_template:
            import jinja2
            self.message_template = jinja2.Template(message_template)

        self.assume_role = assume_role

    def __call__(self, model) -> None:
        """Send a notification as configured."""
        utcnow = self.di.build('utcnow')
        if self.when and not self.di.call_function(self.when, model=model):
            return

        if self.assume_role:
            boto3 = self.assume_role(self.boto3)
        else:
            boto3 = self.boto3

        ses = boto3.client("ses")
        tos = self._resolve_destination("to", model)
        try:
            response = ses.send_email(
                Destination={
                    "ToAddresses": tos,
                    "CcAddresses": self._resolve_destination("cc", model),
                    "BccAddresses": self._resolve_destination("bcc", model),
                },
                Message={
                    "Body": {
                        "Html": {
                            "Charset": "utf-8",
                            "Data": self._resolve_message_as_html(model, utcnow),
                        },
                        "Text": {
                            "Charset": "utf-8",
                            "Data": self._resolve_message_as_text(model, utcnow),
                        },
                    },
                    "Subject": {
                        "Charset": "utf-8",
                        "Data": self._resolve_subject(model, utcnow)
                    },
                },
                Source=self.sender,
            )
        except ClientError as e:
            raise e

    def _resolve_destination(self, name: str, model: clearskies.Model) -> List[str]:
        """
        Return a list of to/cc/bcc addresses.

        Each entry can be:

         1. An email address
         2. The name of a column in the model that contains an email address
        """
        resolved = []
        destinations = self.destinations[name]
        for destination in destinations:
            if "@" in destination:
                resolved.append(destination)
                continue
            resolved.append(model.get(name))
        return resolved

    def _resolve_message_as_html(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the HTML for a message."""
        if self.message:
            return self.message

        if self.message_template:
            return str(
                self.message_template.render(model=model, now=now, **self.more_template_variables(), text_in_html=True)
            )

        return ""

    def _resolve_message_as_text(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the text for a message."""
        if self.message:
            return self.message

        if self.message_template:
            return str(self.message_template.render(model=model, now=now, **self.more_template_variables()))

        return ""

    def _resolve_subject(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the subject for a message."""
        if self.subject:
            return self.subject

        if self.subject_template:
            return str(self.subject_template.render(model=model, now=now, **self.more_template_variables()))

        return ""

    def more_template_variables(self):
        more_variables = {}
        for dependency_name in self.dependencies_for_template:
            more_variables[dependency_name] = self.di.build(dependency_name)
        return more_variables
