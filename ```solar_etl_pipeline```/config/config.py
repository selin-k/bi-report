# filename: config/config.py
import yaml
import os

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Load configuration from the YAML file."""
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path, 'r') as ymlfile:
            self.config = yaml.safe_load(ymlfile)

    def get(self, key, default=None):
        """Get a configuration value by key."""
        return self.config.get(key, default)

# Usage example:
# config = Config()
# project_name = config.get('ProjectName')