import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, KeysView

from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import connection, cursor
from psycopg2.extras import DictRow

from core.decorators import backoff
from core.settings import BATCH_SIZE


class UpdatesNotFoundError(Exception):
    """Обновлений не обнаружено."""


@dataclass
class PostgresExtractor(object):
    """Класс для получения данных из PostgreSQL."""

    postgres: connection

    TABLES = ('film_work', 'person', 'genre')

    @backoff(errors=(InterfaceError, OperationalError))
    def select_table(self, table: str, timestamp: datetime) -> cursor:
        """Запрашивает обновления в таблице на текущий момент.

        Args:
            table: Название таблицы
            timestamp: Дата и время последнего обновления

        Returns:
            cursor: Объект курсора
        """
        curs = self.postgres.cursor()
        query = """
            SELECT *
            FROM {table}
            WHERE modified > TIMESTAMP '{timestamp}'
            ORDER BY modified;
        """
        curs.execute(query.format(table=table, timestamp=timestamp))
        return curs

    @backoff(errors=(InterfaceError, OperationalError))
    def get_updates(self, timestamp: datetime) -> Iterator[tuple[str, list]]:
        """Извлекает новые данные с момента последнего обновления.

        Args:
            timestamp: Дата и время последнего обновления

        Raises:
            UpdatesNotFoundError: Обновлений не обнаружено

        Yields:
            tuple[str, list]: Генерирует кортеж с названием таблицы и пачкой данных из неё
        """
        updates = {table: self.select_table(table, timestamp) for table in self.TABLES}
        if not any(curs.rowcount for curs in updates.values()):
            raise UpdatesNotFoundError
        for table, curs in updates.items():
            while data := curs.fetchmany(BATCH_SIZE):
                yield (table, data)
            curs.close()

    @backoff(errors=(InterfaceError, OperationalError))
    def get_film_work_ids(self, table: str, data: list[DictRow]) -> Iterator[DictRow]:
        """Извлекает id фильмов, в которых произошли изменения.

        Args:
            table: Название таблицы
            data: Список данных в формате PostgreSQL

        Yields:
            DictRow: Данные в формате PostgreSQL с id фильма, который нужно обновить
        """
        if table in {'person', 'genre'}:
            with self.postgres.cursor() as curs:
                film_ids = ["'{0}'".format(row['id']) for row in data]
                query = """
                    SELECT fw.id
                    FROM film_work fw
                    LEFT JOIN {table}_film_work gfw ON gfw.film_work_id = fw.id
                    WHERE gfw.{table}_id IN ({film_ids})
                    ORDER BY fw.modified;
                """
                curs.execute(query.format(table=table, film_ids=', '.join(film_ids)))
                yield from curs
        else:
            yield from data

    @backoff(errors=(InterfaceError, OperationalError))
    def get_movie_data(self, film_ids: KeysView[uuid.UUID]) -> Iterator[DictRow]:
        """Извлекает всю нужную информацию для записи в индекс Elasticsearch с названием `movies`.

        Args:
            film_ids: ключи словаря с id фильмов

        Yields:
            DictRow: Данные в формате PostgreSQL со всей нужной информацией о фильмах
        """
        with self.postgres.cursor() as curs:
            film_ids = ["'{0}'".format(film_id) for film_id in film_ids]
            query = """
                SELECT
                    fw.id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    pfw.role,
                    p.id as person_id,
                    p.full_name,
                    g.name as genre_name
                FROM film_work fw
                LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN person p ON p.id = pfw.person_id
                LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN genre g ON g.id = gfw.genre_id
                WHERE fw.id IN ({film_ids})
            """
            curs.execute(query.format(film_ids=', '.join(film_ids)))
            yield from curs
