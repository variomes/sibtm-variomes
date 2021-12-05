import os
from pathlib import Path

import configparser

class Configuration():
    '''
    The Configuration loads settings defined in a .config file into a python object

    Parameters
    ----------
    conf_mode : str
        name of the configuration file to load

    Attributes
    ----------
    settings: dict
        a set of settings
    errors: list
        stores a list of errors with a json format

    '''

    def __init__(self, conf_mode="prod"):
        ''' Load the configuration file in the configparser object and parse settings '''

        # Initiate errors
        self.errors = []

        # Define file name
        config_file_name = "config-variomes"
        if conf_mode != "prod":
            config_file_name = "config-variomes-"+conf_mode

        # Get the configuration file name
        config_folder = os.path.join(os.path.expanduser("~"), '.config', 'sibtm')
        self.config_file_path = os.path.join(config_folder, config_file_name + ".ini")

        # Check if file exist
        if os.path.exists(self.config_file_path):

            # Load the configuration file
            ini = configparser.ConfigParser()
            ini.read(self.config_file_path)

            # Load settings from config files
            self.loadSettings(ini)

        # Otherwise return an error
        else:
            self.errors.append({"level": "fatal", "service":"configuration", "description": "Configuration file not found", "details":config_file_path})


    def loadSettings(self, ini):
        ''' Load settings from configuration file to a dictionary '''

        # Get settings
        self.settings = {}

        for section in ini.sections():
            self.settings[section] = {}

            for key in ini[section]:
                type_tag = key[:2]
                if type_tag == "i_":
                    self.settings[section][key[2:]] = ini.getint(section, key)
                if type_tag == "b_":
                    self.settings[section][key[2:]] = ini.getboolean(section, key)
                if type_tag == "l_":
                    self.settings[section][key[2:]] = ini.get(section, key).split(",")
                if type_tag == "f_":
                    self.settings[section][key[2:]] = ini.getfloat(section, key)
                if type_tag == "s_":
                    self.settings[section][key[2:]] = ini.get(section, key)
                if type_tag == "r_":
                    self.settings[section][key[2:]] = ini.get(section, key)
                    Path(ini.get(section, key)).mkdir(parents=True, exist_ok=True)
