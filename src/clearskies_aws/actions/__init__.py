from typing import Optional
import clearskies
from .assume_role import AssumeRole
from .ses import SES
#from .sns import SNS
from .sqs import SQS
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
def sqs(
    queue_url: str = '',
    queue_url_environment_key: str = '',
    queue_url_callable: str = '',
    message_callable=None,
    when=None,
    assume_role=None,
):
    return clearskies.BindingConfig(
        SQS,
        queue_url=queue_url,
        queue_url_environment_key=queue_url_environment_key,
        queue_url_callable=queue_url_callable,
        message_callable=message_callable,
        when=when,
        assume_role=assume_role,
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
