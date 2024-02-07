import sys
import os
from dotenv import load_dotenv, dotenv_values

from loguru import logger

LOGGER_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | " + \
                "<level>{level: <8}</level> | " + \
                "<green>{extra}</green><level>: {message}</level>"

logger.remove(0)
logger.add(sink="logs/log.log", level="INFO", format=LOGGER_FORMAT, backtrace=False, diagnose=False, rotation='1 GB')
logger.add(sys.stdout, level="INFO", format=LOGGER_FORMAT, backtrace=False, diagnose=False)

DOTENV_PATH = "dev.env"

load_dotenv(dotenv_path=DOTENV_PATH)
logger.info(f"Loaded envs from {DOTENV_PATH}: {dotenv_values(dotenv_path=DOTENV_PATH)}")
