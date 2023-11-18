# filename: config/config.py
import yaml

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            with open('config/config.yaml', 'r') as f:
                cls._instance.config = yaml.safe_load(f)
        return cls._instance

    @staticmethod
    def get_config():
        return Config()._config

# Usage example:
# config = Config.get_config()
# print(config['project_name'])  # Outputs: solar_analytics