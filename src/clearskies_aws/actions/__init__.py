import clearskies
#from .assume_role import AssumeRole
from .ses import SES
#from .sns import SNS
#from .sqs import SQS
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
