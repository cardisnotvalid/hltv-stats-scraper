import sys
from loguru import logger
from datetime import datetime

from stats_scraper.paths import LOG_DIR
from stats_scraper.utils import load_config


DEBUG = load_config()["debug"]

LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> - <level>{level: <7}</level> - <white>{message}</white>"
CONSOLE_FORMAT = "<green>{time:HH:mm:ss}</green> - <level>{level: <7}</level> - <white>{message}</white>"


def setup_logger(debug: bool):
    logger.remove()
    filename = f"{datetime.now().strftime('%d-%m-%Y')}.log"
    filepath = LOG_DIR / filename
    logger.add(filepath, format=LOG_FORMAT, level="DEBUG", rotation="1 day")
    logger.add(sys.stderr, colorize=True, format=CONSOLE_FORMAT, level="DEBUG" if debug else "INFO")


setup_logger(DEBUG)