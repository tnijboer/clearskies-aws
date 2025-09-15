import os

import clearskies


class IAMDBAuth(clearskies.di.AdditionalConfig):
    def provide_boto3(self):
        import boto3

        return boto3

    def provide_connection_details(self, environment, boto3):
        """
        Make configuration values and environment variables customizable.

        Allows both the values and the environment variable names to be set for flexible configuration.

        Returns:
            dict: Connection details for IAM DB authentication.
        """
        endpoint = environment.get("db_endpoint")
        username = environment.get("db_username")
        database = environment.get("db_database")
        region = environment.get("AWS_REGION")
        ssl_ca_bundle_name = environment.get("ssl_ca_bundle_filename")
        os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

        rds_api = boto3.Session().client("rds")
        rds_token = rds_api.generate_db_auth_token(DBHostname=endpoint, Port="3306", DBUsername=username, Region=region)

        return {
            "username": username,
            "password": rds_token,
            "host": endpoint,
            "database": database,
            "ssl_ca": ssl_ca_bundle_name,
        }
