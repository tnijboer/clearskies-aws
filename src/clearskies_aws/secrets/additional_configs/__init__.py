from .mysql_connection_dynamic_producer_via_ssh_cert_bastion import MySQLConnectionDynamicProducerViaSSHCertBastion
from .mysql_connection_dynamic_producer_via_ssm_bastion import MySQLConnectionDynamicProducerViaSSMBastion
from .iam_db_auth import IAMDBAuth
from .iam_db_auth_with_ssm import IAMDBAuthWithSSM
def mysql_connection_dynamic_producer_via_ssh_cert_bastion(
    producer_name=None,
    bastion_host=None,
    bastion_name=None,
    bastion_region=None,
    bastion_username=None,
    public_key_file_path=None,
    cert_issuer_name=None,
    database_host=None,
    database_name=None,
    local_proxy_port=None,
):
    return MySQLConnectionDynamicProducerViaSSHCertBastion(
        producer_name=producer_name,
        bastion_host=bastion_host,
        bastion_name=bastion_name,
        bastion_region=bastion_region,
        bastion_username=bastion_username,
        cert_issuer_name=cert_issuer_name,
        public_key_file_path=public_key_file_path,
        database_host=database_host,
        database_name=database_name,
        local_proxy_port=local_proxy_port,
    )
def mysql_connection_dynamic_producer_via_ssm_bastion(
    producer_name=None,
    bastion_instance_id=None,
    bastion_name=None,
    bastion_region=None,
    bastion_username=None,
    public_key_file_path=None,
    database_host=None,
    database_name=None,
    local_proxy_port=None,
):
    return MySQLConnectionDynamicProducerViaSSMBastion(
        producer_name=producer_name,
        bastion_instance_id=bastion_instance_id,
        bastion_name=bastion_name,
        bastion_region=bastion_region,
        bastion_username=bastion_username,
        public_key_file_path=public_key_file_path,
        database_host=database_host,
        database_name=database_name,
        local_proxy_port=local_proxy_port,
    )
def iam_db_auth():
    return IAMDBAuth()
def iam_db_auth_with_ssm():
    return IAMDBAuthWithSSM()
