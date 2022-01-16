# clearskies-aws

clearskies bindings for working in AWS, which means additional:

 - backends (DynamoDB)
 - Secret/environment integrations (parameter store/secret manager)
 - DB connectivity via IAM auth
 - Contexts (ALB, HTTP API Gateway, Rest API Gateway, direct Lambda invocation)

# Installation, Documentation, and Usage

To install:

```
pip3 install clear-skies-aws
```

# Usage

Anytime you use a context from `clearskies-aws`, the default dependencies are adjust to:

 1. Provide `dynamo_db_backend` as an allowed backend
 2. Configure clearskies to use SSM Parameter Store as the default secrets "manager".

In both cases you must provide the AWS region for your resources, which you do by setting the `AWS_REGION` environment variable (either in an actual environment variable or in your `.env` file).

## Paramter Store

To use the SSM parameter store you just inject the `secrets` variable into your callables:

```
import clearskies_aws

def parameter_store_demo(secrets):
    return secrets.get('/path/to/parameter')

execute_demo_in_elb = clearskies_aws.contexts.lambda_elb(parameter_store_demo)

def lambda_handler(event, context):
    return execute_demo_in_elb(event, context)
```

Also, per default behavior, clearskies can fetch things from your secret manager if specified in your environment/.env file.  For instance, if your database password is stored in parameter store, then you can reference it from your `.env` file with a standard cursor backend:

```
db_host = "path-to-aws.rds"
db_username = "sql_username"
db_password = "secret://path/to/password/in/parameter/store"
db_database = "sql_database_name"
```

## Secret Manager

If desired, you can swap out the parameter store integration for secret manager.  Just remember that you can configure parameter store to fetch secrets from secret manager, so you might be best off doing that and sticking with the default parameter store integration.  Still, if you want to use secret manager, you just configure it in your application or context:

```
import clearskies_aws

def secret_manager_demo(secrets):
    return secrets.get('SecretARNFromSecretManager')

execute_demo_in_elb = clearskies_aws.contexts.lambda_elb(
    parameter_store_demo,
    bindings={'secrets': clearskies_aws.secrets.SecretManager},
)

def lambda_handler(event, context):
    return execute_demo_in_elb(event, context)
```

## Contexts

clearskies_aws adds the following contexts:

| name/import                                   | Usage                             |
|-----------------------------------------------|-----------------------------------|
| `clearskies_aws.contexts.lambda_api_gateway`  | Lambdas behind a Rest API Gateway |
| `clearskies_aws.contexts.lambda_elb`          | Lambdas behind a simple ELB/ALB   |
| `clearskies_aws.contexts.lambda_http_gateway` | Lambdas behind an HTTP Gateway    |
| `clearskies_aws.contexts.lambda_inocation`    | Lambdas invoked directly          |

## IAM DB Auth

For non-serverless RDS databases, AWS supports login via IAM.  You have to provide a few additional details in your environment to make this work:

| name                     | value                                                                                                                 |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `AWS_REGION`             | The region your database is in (e.g. `us-east-1`)                                                                     |
| `db_endpoint`            | The endpoint from your database (available in RDS)                                                                    |
| `db_username`            | The username to use to connect                                                                                        |
| `db_database`            | The name of the database                                                                                              |
| `ssl_ca_bundle_filename` | Path to the appropriate SSL bundle (see https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html) |

and then you have to enable it in your application/context configuration:

```
import clearskies_aws

def cursor_via_iamdb_auth(cursor):
    print('I connected successfully!')

execute_demo_in_elb = clearskies_aws.contexts.lambda_elb(
    cursor_via_iamdb_auth,
    additional_configs=[clearskies_aws.di.IAMDBAuth]
)

def lambda_handler(event, context):
    return execute_demo_in_elb(event, context)
```

Of course normally you wouldn't want to interact with it directly.  Adding `IAMDBAuth` to your `additional_configs` and setting up the necessary environemnt variables will be sufficient that any models that use the `cursor_backend` will connect via IAM DB Auth, rather than using hard-coded passwords.

## IAM DB Auth with SSM Bastion

Coming shortly
