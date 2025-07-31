import clearskies

import clearskies_aws


class Cli(clearskies_aws.contexts.Context, clearskies.contexts.Cli):
    """
    Run an application via a CLI command.

    Extend from the core CLI context,
    but with an override of the DI to use clearskies_aws.di.Di().
    """
