import uuid
from typing import ClassVar, List

from psycopg2.extras import DictCursor
from pydantic import BaseModel, BaseSettings, Field


class Person(BaseModel):
    """Класс для валидации данных персоны."""

    id: uuid.UUID
    name: str = Field(alias='full_name')
    _index: ClassVar[str] = 'persons'


class Genre(BaseModel):
    """Класс для валидации данных жанра."""

    id: uuid.UUID
    name: str
    description: str
    _index: ClassVar[str] = 'genres'


class Movie(BaseModel):
    """Класс для валидации данных фильма."""

    id: uuid.UUID
    imdb_rating: float
    genre: List[str]
    title: str
    description: str
    director: List[str] = Field(alias='directors_names')
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]
    _index: ClassVar[str] = 'movies'

    @classmethod
    def properties(cls, **kwargs) -> dict:
        """Возвращает схему модели фильма с её характеристиками.

        Args:
            kwargs: Необязательные именованные аргументы

        Returns:
            dict: Словарь со схемой модели
        """
        properties = {}
        for field, value in cls.schema(**kwargs)['properties'].items():
            if value['type'] == 'string':
                properties[field] = ''
            if value['type'] == 'array':
                properties[field] = []
            if value['type'] == 'number':
                properties[field] = 0
        return properties


class ElasticSettings(BaseSettings):
    """Класс для валидации настроек подключения к Elasticsearch."""

    host: str = Field(env='es_host')
    port: int = Field(env='es_port')


class PostgresSettings(BaseSettings):
    """Класс для валидации настроек подключения к PostgreSQL."""

    dbname: str
    user: str
    password: str
    host: str = Field(env='pg_host')
    port: int = Field(env='pg_port')
    cursor_factory: type = Field(default=DictCursor)
    options: str = Field(default='-c search_path=content')


class RedisSettings(BaseSettings):
    """Класс для валидации настроек подключения к Redis."""

    db: int
    host: str = Field(env='rds_host')
    port: int = Field(env='rds_port')
