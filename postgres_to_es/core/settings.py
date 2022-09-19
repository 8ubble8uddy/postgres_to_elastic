import logging
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from psycopg2.extras import DictRow

from core.validations import ElasticSettings, PostgresSettings, RedisSettings

load_dotenv()

ES_PARAMS = ElasticSettings(_env_file='.env').dict()

PG_PARAMS = PostgresSettings(_env_file='.env').dict()

RDS_PARAMS = RedisSettings(_env_file='.env').dict()

FILE_SIZE = 50000000

logger = logging.getLogger('__main__')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('main.log', maxBytes=FILE_SIZE, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

BATCH_SIZE = 100

PostgresRow = DictRow
