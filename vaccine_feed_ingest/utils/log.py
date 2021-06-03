import logging
import pathlib
from logging import Logger

LOG_FORMAT = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
# Use RFC-3339 for consistency
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
LOG_LEVEL = logging.INFO

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)

utils_dir = pathlib.Path(__file__).parent
root_dir = utils_dir.parent


def getLogger(file_path: str) -> Logger:
    """
    Returns a configured logger for the given the module `__file__`

    Example usage:

    ```python
    from vaccine_feed_ingest.utils.log import getLogger

    logger = getLogger(__file__)
    ```
    """
    relative_path = pathlib.Path(file_path).relative_to(root_dir)
    logger = logging.getLogger(str(relative_path))

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger
