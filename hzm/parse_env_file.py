from environs import Env
from marshmallow.validate import Email, Length, OneOf


def parse_env_file():
    """
    parse .env file present in current directory or above (recurse upwards) using environs package
    :return: environment name value dict
    """

    env = Env()
    try:
        env.read_env()
    except Exception:
        err_mesg = "exception during read env file"
        raise Exception(err_mesg)

    env.str("IRF_DATABASE", validate=Length(min=1))
    env.str("HAZMAT_DATABASE", validate=Length(min=1))

    return env
