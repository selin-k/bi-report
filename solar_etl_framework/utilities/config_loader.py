# filename: utilities/config_loader.py
import yaml

class ConfigLoader:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config_data = self.load_config()

    def load_config(self):
        """Loads the YAML configuration file."""
        try:
            with open(self.config_path, 'r') as config_file:
                return yaml.safe_load(config_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {self.config_path}")
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(f"Error parsing configuration file: {exc}")

    def get(self, key, default=None):
        """Retrieves a value from the configuration data."""
        return self.config_data.get(key, default)

# Example usage:
# config_loader = ConfigLoader('path/to/config.yaml')
# database_config = config_loader.get('database')