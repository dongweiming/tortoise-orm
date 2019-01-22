import asyncio
import logging
from functools import wraps
from typing import List, Optional, SupportsInt  # noqa

import aiomysql
import pymysql
from pypika import MySQLQuery

from tortoise.backends.base.client import (BaseDBAsyncClient,
                                           BaseTransactionWrapper)
from tortoise.backends.mysql.executor import MySQLExecutor
from tortoise.backends.mysql.schema_generator import MySQLSchemaGenerator
from tortoise.exceptions import (DBConnectionError, IntegrityError, OperationalError,
                                 TransactionManagementError)
from tortoise.transactions import current_transaction_map


def translate_exceptions(func):
    @wraps(func)
    async def wrapped(self, query, *args):
        try:
            return await func(self, query, *args)
        except (pymysql.err.OperationalError, pymysql.err.ProgrammingError,
                pymysql.err.DataError, pymysql.err.InternalError,
                pymysql.err.NotSupportedError) as exc:
            raise OperationalError(exc)
        except pymysql.err.IntegrityError as exc:
            raise IntegrityError(exc)
    return wrapped


class MySQLClient(BaseDBAsyncClient):
    query_class = MySQLQuery
    executor_class = MySQLExecutor
    schema_generator = MySQLSchemaGenerator

    def __init__(self, user: str, password: str, database: str, host: str, port: SupportsInt,
                 **kwargs) -> None:
        super().__init__(**kwargs)

        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = int(port)  # make sure port is int type

        self._pool = None

        self._transaction_class = type(
            'TransactionWrapper', (TransactionWrapper, self.__class__), {}
        )

    async def create_connection(self, with_db: bool) -> None:
        return await self.create_pool(with_db)

    async def create_pool(self, with_db: bool) -> None:
        template = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'db': self.database if with_db else None,
            'autocommit': True
        }
        loop = asyncio.get_event_loop()

        try:
            self._pool = await aiomysql.create_pool(pool_recycle=300,
                                                    loop=loop, **template)
            self.log.debug(
                'Created pool %s with params: user=%s database=%s host=%s port=%s',
                self._pool, self.user, self.database, self.host, self.port
            )
        except pymysql.err.OperationalError:
            raise DBConnectionError(
                "Can't connect to MySQL server: "
                'user={user} database={database} host={host} port={port}'.format(
                    user=self.user, database=self.database, host=self.host, port=self.port
                )
            )

    async def close(self) -> None:
        if self._pool:  # pragma: nobranch
            self._pool.close()
            await self._pool.wait_closed()
            self.log.debug(
                'Closed pool %s with params: user=%s database=%s host=%s port=%s',
                self._pool, self.user, self.database, self.host, self.port
            )
            self._pool = None

    async def db_create(self) -> None:
        await self.create_pool(with_db=False)
        await self.execute_script(
            'CREATE DATABASE {}'.format(self.database)
        )
        await self.close()

    async def db_delete(self) -> None:
        await self.create_pool(with_db=False)
        try:
            await self.execute_script('DROP DATABASE {}'.format(self.database))
        except pymysql.err.DatabaseError:  # pragma: nocoverage
            pass
        await self.close()

    def _in_transaction(self):
        return self._transaction_class(self.connection_name, self._connection)

    @translate_exceptions
    async def execute_insert(self, query: str, values: list) -> int:
        self.log.debug('%s: %s', query, values)
        with (await self._pool) as connection:
            async with connection.cursor() as cursor:
                # TODO: Use prepared statement, and cache it
                await cursor.execute(query, values)
                return cursor.lastrowid  # return auto-generated id

    @translate_exceptions
    async def execute_query(self, query: str) -> List[aiomysql.DictCursor]:
        self.log.debug(query)
        with (await self._pool) as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                return await cursor.fetchall()

    @translate_exceptions
    async def execute_script(self, query: str) -> None:
        self.log.debug(query)
        with (await self._pool) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query)


class TransactionWrapper(MySQLClient, BaseTransactionWrapper):
    def __init__(self, connection_name, connection):
        self.connection_name = connection_name
        self._connection = connection
        self.log = logging.getLogger('db_client')
        self._transaction_class = self.__class__
        self._finalized = False
        self._old_context_value = None

    async def start(self):
        await self._connection.begin()
        current_transaction = current_transaction_map[self.connection_name]
        self._old_context_value = current_transaction.get()
        current_transaction.set(self)

    async def commit(self):
        if self._finalized:
            raise TransactionManagementError('Transaction already finalised')
        self._finalized = True
        await self._connection.commit()
        current_transaction_map[self.connection_name].set(self._old_context_value)

    async def rollback(self):
        if self._finalized:
            raise TransactionManagementError('Transaction already finalised')
        self._finalized = True
        await self._connection.rollback()
        current_transaction_map[self.connection_name].set(self._old_context_value)
