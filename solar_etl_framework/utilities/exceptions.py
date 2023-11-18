# filename: utilities/exceptions.py
class DataIngestionError(Exception):
    """Exception raised for errors in the data ingestion process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataCurationError(Exception):
    """Exception raised for errors in the data curation process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataTransformationError(Exception):
    """Exception raised for errors in the data transformation process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DataVisualizationError(Exception):
    """Exception raised for errors in the data visualization process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ConfigurationError(Exception):
    """Exception raised for errors related to configuration loading."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)