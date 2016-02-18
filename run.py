import json


config = {}


if __name__ == '__main__':
    with open('config.json') as config_file:
        config = json.load(config_file)
