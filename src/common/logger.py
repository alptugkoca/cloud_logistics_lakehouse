import logging
import os
from datetime import datetime


def get_logger(name: str = "data_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Prevent duplicate handlers

    logger.setLevel(logging.INFO)

    # Create logs directory if not exists
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # File handler (timestamped logs)
    log_file = os.path.join(
        log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger