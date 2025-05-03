# utils/error_handling.py
import logging
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

class FrameworkError(Exception):
    """Base class for all framework exceptions"""
    pass

class DataConnectorError(FrameworkError):
    """Errors related to data connectors"""
    pass

class DeltaTableError(FrameworkError):
    """Errors related to Delta Lake operations"""
    pass

class ValidationError(FrameworkError):
    """Data validation failures"""
    pass

class RetryableError(FrameworkError):
    """Transient errors that warrant retries"""
    pass

class SecretManagerError(FrameworkError):
    """Errors related to secret management"""
    pass

def handle_errors(func: Callable) -> Callable:
    """Decorator to handle and log framework errors"""
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except DataConnectorError as e:
            logger.error(f"Data connector error: {str(e)}")
            raise
        except ValidationError as e:
            logger.error(f"Data validation failed: {str(e)}")
            raise
        except DeltaTableError as e:
            logger.error(f"Delta Lake operation failed: {str(e)}")
            raise
        except RetryableError as e:
            logger.warning(f"Retryable error occurred: {str(e)}")
            raise
        except Exception as e:
            logger.exception("Unhandled exception occurred")
            raise FrameworkError(f"Unexpected error: {str(e)}") from e
    return wrapper