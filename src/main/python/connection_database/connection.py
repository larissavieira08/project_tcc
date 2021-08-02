# import psycopg2
import psycopg2
import sqlalchemy
from decouple import config


def connect_database(): #conexão com o banco de origem
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(user=config('user'),
                                password=config('password'),
                                host=config('host'),
                                port=config('port'),
                                database=config('database'))
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None


def connect_database_dw(): #conexão com o DW
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(user=config('user'),
                                password=config('password'),
                                host=config('host'),
                                port=config('port'),
                                database=config('database_dw'))

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None


def connection_url():
    return sqlalchemy.create_engine('postgresql://'+config('user')+':'+config('password')+'@'+config('host')+':'+config('port')+'/'+config('database'))


def connection_dw_url():
    return sqlalchemy.create_engine('postgresql://'+config('user')+':'+config('password')+'@'+config('host')+':'+config('port')+'/'+config('database_dw'))
