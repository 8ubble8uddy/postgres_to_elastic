import time
from functools import wraps
from typing import Any, Callable

from core.settings import logger


def backoff(errors: tuple, start_sleep_time=0.1, factor=2, border_sleep_time=10) -> Callable:
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.

    Args:
        errors: Ошибки, которые нужно обработать
        start_sleep_time: Начальное время повтора
        factor: Во сколько раз нужно увеличить время ожидания
        border_sleep_time: Граничное время ожидания

    Returns:
        Callable: Декорируемая функция
    """
    def decorator(func) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = start_sleep_time
            while True:
                try:
                    conn = func(*args, **kwargs)
                except errors as message:
                    logger.error('Нет соединения: {0}!'.format(message))
                    if delay < border_sleep_time:
                        delay *= 2
                    logger.error('Повторное подключение через {0}.'.format(delay))
                    time.sleep(delay)
                else:
                    return conn
        return wrapper
    return decorator
