import clearskies
import os
class IAMDBAuth(clearskies.di.AdditionalConfig):
    _boto3 = None
    _environment = None

    def __init__(self, boto3, environment):
        self._boto3 = boto3
        self._environment = environment

    def provide_connection_no_autocommit(self):
        return self._connect(False)

    def provide_connection(self):
        return self._connect(True)

    def _connect(self, autocommit):
        import mysql
        endpoint = self._environment.get('db_endpoint')
        username = self._environment.get('db_username')
        database = self._environment.get('db_database')
        region = self._environment.get('AWS_REGION')
        ssl_ca_bundle_name = self._environment.get('ssl_ca_bundle_filename')
        os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

        rds_api = self._boto3.Session().client('rds')
        rds_token = rds_api.generate_db_auth_token(DBHostname=endpoint, Port='3306', DBUsername=username, Region=region)

        return pymysql.connect(
            host=endpoint,
            user=username,
            password=rds_token,
            port=3306,
            database=database,
            ssl_ca=ssl_ca_bundle_name,
            connect_timeout=2,
            autocommit=autocommit,
            cursorclass=pymysql.cursors.DictCursor
        )
