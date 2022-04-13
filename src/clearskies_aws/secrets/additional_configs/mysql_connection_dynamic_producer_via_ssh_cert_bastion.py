from clearskies.secrets.additional_configs import MySQLConnectionDynamicProducerViaSSHCertBastion as Base
from pathlib import Path
import socket
import subprocess
import os
import time
class MySQLConnectionDynamicProducerViaSSHCertBastion(Base):
    _config = None
    _boto3 = None

    def __init__(
        self,
        producer_name=None,
        bastion_region=None,
        bastion_name=None,
        bastion_host=None,
        bastion_username=None,
        public_key_file_path=None,
        local_proxy_port=None,
        cert_issuer_name=None,
        database_host=None,
        database_name=None
    ):
        # not using kwargs because I want the argument list to be explicit
        self.config = {
            'producer_name': producer_name,
            'bastion_host': bastion_host,
            'bastion_region': bastion_region,
            'bastion_name': bastion_name,
            'bastion_username': bastion_username,
            'public_key_file_path': public_key_file_path,
            'local_proxy_port': local_proxy_port,
            'cert_issuer_name': cert_issuer_name,
            'database_host': database_host,
            'database_name': database_name,
        }

    def provide_connection_details(self, environment, secrets, boto3):
        self._boto3 = boto3
        return super().provide_connection_details(environment, secrets)

    def _get_bastion_host(self, environment):
        bastion_host = self._fetch_config(environment, 'bastion_host', 'akeyless_mysql_bastion_host', default='')
        bastion_name = self._fetch_config(environment, 'bastion_name', 'akeyless_mysql_bastion_name', default='')
        if bastion_host:
            return bastion_host
        if bastion_name:
            bastion_region = self._fetch_config(environment, 'bastion_region', 'akeyless_mysql_bastion_region')
            return self._public_ip_from_name(bastion_name, bastion_region)
        raise ValueError(
            f"I was asked to connect to a database via an AKeyless dynamic producer through an SSH bastion with certificate auth, but I'm missing some configuration. I need either the bastion host or the name of the instance in AWS.  These can be set in the call to `clearskies.backends.akeyless_aws.mysql_connection_dynamic_producer_via_ssh_cert_bastion()` by providing the 'bastion_host' or 'bastion_name' argument, or by setting an environment variable named 'akeyless_mysql_bastion_host' or 'akeyless_mysql_bastion_name'."
        )

    def _public_ip_from_name(self, bastion_name, bastion_region):
        ec2 = self._boto3.client('ec2', region_name=bastion_region)
        response = ec2.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [bastion_name]
                },
                {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                },
            ],
        )
        if not response.get('Reservations'):
            raise ValueError(
                f"Could not find a running instance with the designated bastion name, '{bastion_name}' in region '{bastion_region}'"
            )
        if not response.get('Reservations')[0].get('Instances'):
            raise ValueError(
                f"Could not find a running instance with the designated bastion name, '{bastion_name}' in region '{bastion_region}'"
            )
        instance = response.get('Reservations')[0].get('Instances')[0]
        if not instance.get('PublicIpAddress'):
            raise ValueError(
                f"I found the bastion instance with a name of '{bastion_name}' in region '{bastion_region}', but it doesn't have a public IP address"
            )
        return instance.get('PublicIpAddress')
