from configparser import ConfigParser

def config(filename: str, section: str) -> dict:
    """Load and return the config file values
    Parameters:
    filename (str): Name of config file
    section (str): Name of section from which to load variables from the config file
    Returns:
    json: key Value pairs of the config file
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
