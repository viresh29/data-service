import logging
import time
from functools import wraps
from exceptions import (s3DataUploaderException, snowflakeException)


def s3_retry_decorator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        for _ in range(self.timeout_counter):
            try:
                return func(self, *args, **kwargs)
            except FileNotFoundError as e:
                logging.error(
                    "FileNotFoundError during {} ".format(func.__name__))
                logging.error(e)
            except Exception as e:
                logging.error(
                    "Exception during {}".format(func.__name__))
                logging.error(e)
            time.sleep(self.timeout_sleep)
        logging.error("{} failed {} times".format(func.__name__,
                                                  self.timeout_counter))
        raise s3DataUploaderException(
            "{} failed {} times".format(func.__name__, self.timeout_counter))

    return wrapper


def snowflake_retry_decorator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        for _ in range(self.timeout_counter):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logging.error(
                    "Exception during {}".format(func.__name__))
                logging.error(e)
            time.sleep(self.timeout_sleep)
        logging.error("{} failed {} times".format(func.__name__,
                                                  self.timeout_counter))
        raise snowflakeException(
            "{} failed {} times".format(func.__name__, self.timeout_counter))

    return wrapper


def logging_decorator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            logging.info("Task is starting")
            result = func(self, *args, **kwargs)
            logging.info("Task was finished")
            return result
        except BaseException as ex:
            logging.error("Execution failed with {}".format(ex))
            raise ex
    return wrapper
