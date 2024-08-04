import boto3
import clearskies
import datetime

from botocore.exceptions import ClientError
from clearskies.environment import Environment
from clearskies.models import Models
from collections.abc import Sequence
from typing import Any, Callable, List, Optional, Union
from types import ModuleType

from ..di import StandardDependencies
from .assume_role import AssumeRole
from .action_aws import ActionAws
class SES(ActionAws):
    _name = "ses"

    def __init__(self, environment: Environment, boto3: boto3, di: StandardDependencies) -> None:
        """Setup action."""
        super().__init__(environment, boto3, di)

    def configure(
        self,
        sender,
        to: Optional[Union[list, str,  Callable]] = None,
        cc: Optional[Union[list, str,  Callable]] = None,
        bcc: Optional[Union[list, str,  Callable]] = None,
        subject: Optional[str] = None,
        message: Optional[str] = None,
        subject_template: Optional[str] = None,
        message_template: Optional[str] = None,
        subject_template_file: Optional[str] = None,
        message_template_file: Optional[str] = None,
        assume_role: Optional[AssumeRole] = None,
        dependencies_for_template: Optional[list[Any]] = None,
        when: Optional[Callable] = None,
    ) -> None:
        """Configure the rules for this email notification."""
        super().configure(message_callable=None, when=when, assume_role=assume_role)
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
            if type(destination_values) == str or callable(destination_values):
                self.destinations[key] = [destination_values]
            else:
                self.destinations[key] = destination_values
        self.subject = subject
        self.message = message
        self.sender = sender
        self.subject_template = None
        self.message_template = None
        self.dependencies_for_template = dependencies_for_template if dependencies_for_template else []

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

    def _execute_action(self, client: ModuleType, model: Models) -> None:
        """Send a notification as configured."""
        utcnow = self.di.build('utcnow')

        tos = self._resolve_destination("to", model)
        if not tos:
            return
        response = client.send_email(
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
            if callable(destination):
                more = self.di.call_function(destination, model=model)
                if not isinstance(more, list):
                    more = [more]
                for entry in more:
                    if not isinstance(entry, str):
                        raise ValueError(f"I invoked a callable to fetch the '{name}' addresses for model '{model.__class__.__name__}' but it returned something other than a string.  Callables must return a valid email address or a list of email addresses.")
                    if "@" not in entry:
                        raise ValueError(f"I invoked a callable to fetch the '{name}' addresses for model '{model.__class__.__name__}' but it returned a non-email address.  Callables must return a valid email address or a list of email addresses.")
                resolved.extend(more)
                continue
            if "@" in destination:
                resolved.append(destination)
                continue
            resolved.append(model.get(destination))
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

    def more_template_variables(self) -> dict[str, Any]:
        more_variables = {}
        for dependency_name in self.dependencies_for_template:
            more_variables[dependency_name] = self.di.build(dependency_name, cache=True)
        return more_variables
