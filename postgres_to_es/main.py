import time
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extensions import connection
from redis import Redis

from etl_components import extract, load, transform
from core.settings import ES_PARAMS, PG_PARAMS, RDS_PARAMS, logger
from core.state import RedisStorage, State
from core.validations import Genre, Movie, Person


def etl_process(
    postgres: extract.PostgresExtractor,
    data: transform.DataTransform,
    elastic: load.ElasticsearchLoader,
    state: State,
):
    """Запускает внутренние компоненты процесса Extract-Transform-Load.

    Args:
        postgres: Извлекает данные из PostgreSQL
        data: Преобразовывает и хранит промежуточные данные
        elastic: Загружает данные в ElasticSeacrh
        state: Состояние системы
    """
    timestamp = state.read_state('last_updated', datetime.min)
    for table, rows in postgres.get_updates(timestamp):
        if table == 'person':
            elastic.bulk_insert(Person, rows)
        if table == 'genre':
            elastic.bulk_insert(Genre, rows)
        for row in postgres.get_film_work_ids(table, rows):
            data.collector('movie_ids', row['id'])
    for movies in data.batcher('movie_ids'):
        for row in postgres.get_movie_data(movies.keys()):
            data.parser(row, movies.get(row['id']))
        elastic.bulk_insert(Movie, movies.values())


def load_from_postgres(postgres: connection, elastic: Elasticsearch, redis: Redis):
    """Основной метод загрузки данных из PostgreSQL в Elasticsearch.

    Args:
        postgres: соединение с PostgreSQL
        elastic: соединение с ElasticSeacrh
        redis: соединение с Redis
    """
    state = State(RedisStorage(redis))
    while True:
        try:
            etl_process(
                extract.PostgresExtractor(postgres),
                transform.DataTransform(redis),
                load.ElasticsearchLoader(elastic),
                state,
            )
        except extract.UpdatesNotFoundError:
            logger.info('Нет обновлений.')
        else:
            logger.info('Есть обновления!')
            state.write_state('last_updated', datetime.now(tz=ZoneInfo('Europe/Moscow')))
        finally:
            logger.info('Повторный запрос через 1 минуту.')
            time.sleep(60)


if __name__ == '__main__':
    with psycopg2.connect(**PG_PARAMS) as pg_conn:
        with Elasticsearch(**ES_PARAMS) as es_conn:
            with Redis(**RDS_PARAMS) as rds_conn:
                load_from_postgres(pg_conn, es_conn, rds_conn)
