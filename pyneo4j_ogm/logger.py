"""
Logging module.

Logging is controlled by two environment variables:
- PYNEO4J_OGM_LOG_LEVEL: the log level to use. Defaults to `WARNING`.
- PYNEO4J_OGM_ENABLE_LOGGING: whether to enable logging. Defaults to `True`.
"""
import logging
from os import environ

enable_logging = environ.get("PYNEO4J_OGM_ENABLE_LOGGING", "True").lower() == "true"
log_level = int(environ.get("PYNEO4J_OGM_LOG_LEVEL", logging.DEBUG))

logger = logging.getLogger("pyneo4j-ogm")
logger.setLevel(log_level)

handler = logging.StreamHandler()
handler.setLevel(log_level)

formatter = logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

if not enable_logging:
    logger.disabled = True
