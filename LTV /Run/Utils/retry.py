import time
from functools import wraps
from typing import Callable, Any

class RetryError(Exception):
    pass

def retry(max_attempts: int = 2, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Decorador para reintentar operaciones"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                        continue
                    raise RetryError(f"Fallo después de {max_attempts} intentos: {e}") from e
            raise last_exception
        return wrapper
    return decorator