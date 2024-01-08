from configparser import ConfigParser
from Constants.paths import DATABASE_CONFIG_FILE


def config(filename=DATABASE_CONFIG_FILE, section='database') -> dict:
    """parses database_confi.ini to get the parameters and connect to postgres database"""
    parser = ConfigParser()

    parser.read(filename)
    parameters_dic = {}

    if parser.has_section(section):
        params = parser.items(section)

        for param in params:
            parameters_dic[param[0]] = param[1]

    else:
        raise Exception(f"section not found in {filename}")

    return parameters_dic
