import logging


class CustomFormatter(logging.Formatter):
    # ANSI escape codes for colors
    GREEN = '\033[92m'
    RESET = '\033[0m'

    # Customize the formatting for different log levels
    FORMATS = {
        logging.DEBUG: logging.Formatter('%(message)s'),
        logging.INFO: logging.Formatter(GREEN + '%(message)s' + RESET),
        logging.WARNING: logging.Formatter('%(message)s'),
        logging.ERROR: logging.Formatter('%(message)s'),
        logging.CRITICAL: logging.Formatter('%(message)s')
    }


    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = log_fmt if log_fmt else self.FORMATS[logging.INFO]
        return formatter.format(record)


# Set up the root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create a console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())

# Add the console handler to the logger
logger.addHandler(ch)

# Test logging messages
logger.debug("This is a debug message")
logger.info("This is an info message in green")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")
