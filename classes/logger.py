import logging
from logging.handlers import RotatingFileHandler

# basic logging functionality, uses Singleton pattern

class Logger:
    _instance = None  

    def __new__(cls, log_file="./default.log", log_level=logging.INFO):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize_logger(log_file, log_level)
        return cls._instance


    def _initialize_logger(self, log_file, log_level):
        """
        initialize the logger
        """
        self.logger = logging.getLogger("ApplicationLogger")
        self.logger.setLevel(log_level)

        # rotating file handler - rotate log files after they reach certain size
        handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)


    @classmethod
    def get_logger(cls, log_file="./default.log", log_level=logging.INFO):
        """
        set or retrieve the logger instance.
        """
        if cls._instance is None:
            cls(log_file=log_file, log_level=log_level)
        return cls._instance.logger
