from datetime import datetime, timedelta, timezone
import json
import logging
import pathlib
from os import path
from exceptions import ConfigError


def get_json_config(path_to_file):
    logging.info('Attaching configuration {}'.format(path_to_file))

    with open(path_to_file, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.error("invalid config {}".format(path_to_file))
            raise ConfigError("invalid config {}".format(path_to_file))


def get_current_path():
    return str(pathlib.Path().absolute())


def join_path(source_path, relative_path):
    return path.join(source_path, relative_path)


curr_path = get_current_path()
rel_path = 'config/dev.json'
full_path = join_path(source_path=curr_path.replace(
    'dags', ''), relative_path=rel_path)
config = get_json_config(full_path)

SCHEDULE_INTERVAL = timedelta(minutes=5)

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 2, 10, 7, 0).isoformat(),
}

DAG_ID = 'landing-club-ingestion'
