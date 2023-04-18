import configparser
from pathlib import Path


class PropertiesHandler:

    @staticmethod
    def get_properties(value: str) -> str:
        config = configparser.RawConfigParser()
        config.read(f"{Path(__file__).parent.parent.parent}/properties.ini")
        for section in config.sections():
            if config[section].get(value) is not None:
                return config[section].get(value)