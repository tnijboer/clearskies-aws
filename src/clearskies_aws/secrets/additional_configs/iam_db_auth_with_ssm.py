import time
import clearskies
class IAMDBAuthWithSSM(clearskies.di.AdditionalConfig):
    def provide_subprocess(self):
        import subprocess
        return subprocess

    def provide_socket(self):
        import socket
        return socket

    def provide_connection_details(self, environment, subprocess, socket, boto3):
        local_port = self.open_tunnel(environment, subprocess, socket, boto3)

        return {
            'host': '127.0.0.1',
            'database': environment.get('db_database'),
            'username': environment.get('db_username'),
            'password': self.get_password(environment, boto3),
            'ssl_ca': 'rds-cert-bundle.pem',
            'port': local_port,
        }

    def get_password(self, environment, boto3):
        endpoint = environment.get('db_endpoint')
        username = environment.get('db_username')
        region = environment.get('db_region')

        rds_api = boto3.Session().client('rds', region_name=region)
        return rds_api.generate_db_auth_token(DBHostname=endpoint, Port='3306', DBUsername=username, Region=region)

    def open_tunnel(self, environment, subprocess, socket, boto3):
        endpoint = environment.get('db_endpoint')
        region = environment.get('db_region')
        instance_name = environment.get('instance_name')
        local_proxy_port = int(environment.get('local_proxy_port', '9000'))

        ec2_api = boto3.client('ec2', region_name=region)
        running_instances = ec2_api.describe_instances(
            Filters=[{
                'Name': 'tag:Name',
                'Values': [instance_name]
            }, {
                'Name': 'instance-state-name',
                'Values': ['running']
            }],
        )
        instance_ids = []
        for reservation in running_instances['Reservations']:
            for instance in reservation['Instances']:
                instance_ids.append(instance['InstanceId'])

        if len(instance_ids) == 0:
            raise ValueError('Failed to launch SSM tunnel! Cannot find bastion!')

        instance_id = instance_ids.pop()
        self._connect_to_bastion(local_proxy_port, instance_id, endpoint, subprocess, socket)
        return local_proxy_port

    def _connect_to_bastion(self, local_proxy_port, instance_id, endpoint, subprocess, socket):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', local_proxy_port))
        if result == 0:
            sock.close()
            return

        tunnel_command = [
            'aws',
            '--region',
            'us-east-1',
            'ssm',
            'start-session',
            '--target',
            '{}'.format(instance_id),
            '--document-name',
            'AWS-StartPortForwardingSessionToRemoteHost',
            '--parameters={{"host":["{}"], "portNumber":["3306"],"localPortNumber":["{}"]}}'.format(
                endpoint, local_proxy_port
            ),
        ]

        subprocess.Popen(tunnel_command)
        connected = False
        attempts = 0
        while not connected and attempts < 6:
            attempts += 1
            time.sleep(0.5)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', local_proxy_port))
            if result == 0:
                return
        raise ValueError('Failed to launch SSM tunnel with command: ' + ' '.join(tunnel_command))
