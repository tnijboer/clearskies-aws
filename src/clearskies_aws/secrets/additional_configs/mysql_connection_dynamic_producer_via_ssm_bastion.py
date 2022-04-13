from .mysql_connection_dynamic_producer_via_ssh_cert_bastion import MySQLConnectionDynamicProducerViaSSHCertBastion as Base
from pathlib import Path
import socket
import subprocess
import os
import time
class MySQLConnectionDynamicProducerViaSSMBastion(Base):
    _config = None
    _boto3 = None

    def __init__(
        self,
        producer_name=None,
        bastion_region=None,
        bastion_name=None,
        bastion_username=None,
        bastion_instance_id=None,
        public_key_file_path=None,
        local_proxy_port=None,
        database_host=None,
        database_name=None
    ):
        # not using kwargs because I want the argument list to be explicit
        self.config = {
            'producer_name': producer_name,
            'bastion_instance_id': bastion_instance_id,
            'bastion_region': bastion_region,
            'bastion_name': bastion_name,
            'bastion_username': bastion_username,
            'public_key_file_path': public_key_file_path,
            'local_proxy_port': local_proxy_port,
            'database_host': database_host,
            'database_name': database_name,
        }

    def provide_connection_details(self, environment, secrets, boto3):
        self._boto3 = boto3
        if not secrets:
            raise ValueError(
                "I was asked to connect to a database via an AKeyless dynamic producer but AKeyless itself wasn't configured.  Try setting the AKeyless auth method via clearskies.secrets.akeyless_[jwt|saml|aws_iam]_auth()"
            )

        producer_name = self._fetch_config(environment, 'producer_name', 'akeyless_mysql_dynamic_producer')
        bastion_username = self._fetch_config(environment, 'bastion_username', 'mysql_bastion_username', default='ssm')
        bastion_instance_id = self._get_bastion_instance_id(environment)
        public_key_file_path = self._fetch_config(
            environment, 'public_key_file_path', 'mysql_bastion_public_key_file_path'
        )
        local_proxy_port = self._fetch_config(
            environment, 'local_proxy_port', 'akeyless_mysql_bastion_local_proxy_port', default=8888
        )
        database_host = self._fetch_config(environment, 'database_host', 'db_host')
        database_name = self._fetch_config(environment, 'database_name', 'db_database')

        # Create the SSH tunnel (yeah, it's obnoxious)
        self._create_tunnel(
            secrets, bastion_instance_id, bastion_username, bastion_region, public_key_file_path, local_proxy_port,
            database_host
        )

        # and now we can fetch credentials
        credentials = secrets.get_dynamic_secret(producer_name)

        return {
            'username': credentials['user'],
            'password': credentials['password'],
            'host': '127.0.0.1',
            'database': database_name,
            'port': local_proxy_port,
        }

    def _get_bastion_instance_id(self, environment):
        bastion_instance_id = self._fetch_config(
            environment, 'bastion_instance_id', 'mysql_bastion_instance_id', default=''
        )
        bastion_name = self._fetch_config(environment, 'bastion_name', 'mysql_bastion_name', default='')
        if bastion_instance_id:
            return bastion_instance_id
        if bastion_name:
            bastion_region = self._fetch_config(environment, 'bastion_region', 'mysql_bastion_region')
            return self._instance_id_from_name(bastion_name, bastion_region)
        raise ValueError(
            f"I was asked to connect to a database via an AKeyless dynamic producer through an SSH bastion with certificate auth, but I'm missing some configuration. I need either the bastion host or the name of the instance in AWS.  These can be set in the call to `clearskies.backends.akeyless_aws.mysql_connection_dynamic_producer_via_ssh_cert_bastion()` by providing the 'bastion_host' or 'bastion_name' argument, or by setting an environment variable named 'akeyless_mysql_bastion_host' or 'akeyless_mysql_bastion_name'."
        )

    def _instance_id_from_name(self, bastion_name, bastion_region):
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
        return response.get('Reservations')[0].get('Instances')[0]['InstanceId']

    def _create_tunnel(
        self, secrets, bastion_instance_id, bastion_username, bastion_region, public_key_file_path, local_proxy_port,
        database_host
    ):
        # first see if the socket is already open, since we don't close it.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', local_proxy_port))
        if result == 0:
            sock.close()
            return

        # and now we can do this thing.
        tunnel_command = [
            'ssh', '-i', public_key_file_path, '-o', 'ConnectTimeout=2', '-N', '-L',
            f'{local_proxy_port}:{database_host}:3306', '-p', '22', f'{bastion_username}@{bastion_instance_id}'
        ]
        my_env = os.environ.copy()
        my_env['AWS_DEFAULT_REGION'] = bastion_region
        subprocess.Popen(tunnel_command)
        connected = False
        attempts = 0
        while not connected and attempts < 6:
            attempts += 1
            time.sleep(0.5)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', local_proxy_port))
            if result == 0:
                connected = True
        if not connected:
            raise ValueError(
                'Failed to open SSH tunnel.  The following command was used: \n' + ' '.join(tunnel_command)
            )
