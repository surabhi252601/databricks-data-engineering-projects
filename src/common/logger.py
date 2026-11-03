
import logging


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a reusable application logger.

    Args:
        name: Name of the module requesting the logger.

    Returns:
        Configured Python logger.
    """

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.handlers:

        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
