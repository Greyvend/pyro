import json

# global configuration object
settings = {}


def load(file_name):
    global settings
    with open(file_name) as config_file:
        settings = json.load(config_file)
