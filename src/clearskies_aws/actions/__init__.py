import clearskies

from typing import Optional, Callable, Union

from .assume_role import AssumeRole
from .ses import SES
from .sns import SNS
from .sqs import SQS
from .step_function import StepFunction
def ses(
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
):
    return clearskies.BindingConfig(
        SES,
        sender,
        to=to,
        cc=cc,
        bcc=bcc,
        subject=subject,
        subject_template=subject_template,
        subject_template_file=subject_template_file,
        message=message,
        message_template=message_template,
        message_template_file=message_template_file,
        assume_role=assume_role,
        dependencies_for_template=dependencies_for_template,
        when=when,
    )
def sns(
    topic=None,
    topic_environment_key=None,
    topic_callable=None,
    message_callable=None,
    when=None,
):
    return clearskies.BindingConfig(
        SNS,
        topic=topic,
        topic_environment_key=topic_environment_key,
        topic_callable=topic_callable,
        message_callable=message_callable,
        when=when,
    )
def sqs(
    queue_url: str = '',
    queue_url_environment_key: str = '',
    queue_url_callable: Callable = '',
    message_callable: Callable = None,
    when: Callable = None,
    assume_role=None,
    message_group_id: Optional[Union[Callable, str]]=None,
):
    return clearskies.BindingConfig(
        SQS,
        queue_url=queue_url,
        queue_url_environment_key=queue_url_environment_key,
        queue_url_callable=queue_url_callable,
        message_callable=message_callable,
        when=when,
        assume_role=assume_role,
        message_group_id=message_group_id,
    )
def step_function(
    arn: str = "",
    arn_environment_key: str = "",
    arn_callable: Optional[Callable] = None,
    message_callable: Optional[Callable] = None,
    when: Optional[Callable] = None,
    assume_role: Optional[AssumeRole] = None,
    column_to_store_execution_arn: Optional[str] = None,
):
    return clearskies.BindingConfig(
        StepFunction,
        arn=arn,
        arn_environment_key=arn_environment_key,
        arn_callable=arn_callable,
        message_callable=message_callable,
        when=when,
        assume_role=assume_role,
        column_to_store_execution_arn=column_to_store_execution_arn,
    )
def assume_role(
    role_arn: str,
    external_id: str = "",
    role_session_name: str = "",
    duration: int = 3600,
    source: Optional[AssumeRole] = None,
):
    return AssumeRole(
        role_arn,
        external_id=external_id,
        role_session_name=role_session_name,
        duration=duration,
        source=source,
    )
__all__ = [assume_role, AssumeRole, ses, SES, sns, SNS, step_function, StepFunction, sqs, SQS]
