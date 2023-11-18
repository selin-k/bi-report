# filename: utilities/logger.py
import logging

def setup_logger(name, level=logging.INFO):
    """
    Sets up a logger with the given name and level.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger

# Example usage:
# logger = setup_logger(__name__)
# logger.info('This is an info message')