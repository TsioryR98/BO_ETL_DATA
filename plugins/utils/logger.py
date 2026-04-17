import logging


def get_logger(__name__: str) -> logging.Logger:
    """
    Simple logger without persistence for logs
    :param __name__:
    :return: Logger
    """
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        log_handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s"
        )
        log_handler.setFormatter(fmt)
        logger.addHandler(log_handler)
        logger.setLevel(logging.INFO)
    return logger
