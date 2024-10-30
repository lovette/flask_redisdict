# pyright: reportIncompatibleMethodOverride=false
"""Flask extension that allows access to Redis hash as a dictionary.

This module provides:
- RedisDict
"""

from __future__ import annotations

from collections.abc import Collection, Iterator, Mapping, MutableMapping, Sequence
from typing import TYPE_CHECKING, Union
from uuid import uuid4

from flask.json.tag import TaggedJSONSerializer

if TYPE_CHECKING:
    import redis


_RedisDictScalarValuesT = Union[str, int, bool]
RedisDictValuesT = Union[_RedisDictScalarValuesT, Collection[_RedisDictScalarValuesT]]


class RedisDictNoRedisError(ValueError):
    """Exception for RedisDict not having a Redis instance."""

    def __init__(self, *args: object) -> None:
        """Constructor."""
        super().__init__("<{self!r}> has no redis instance.", *args)


class RedisDict(MutableMapping):
    """Acts like a dictionary but reflects item access to a Redis hash.

    Attributes:
        redis (redis.Redis): Redis instance; must be assigned before dictionary is accessed.
        key (string): Hash key; must be assigned before dictionary is accessed.
        max_age (int): Hash key TTL in seconds, None if key does not expire.
        serializer: Serializer to use when storing values.
    """

    serializer = TaggedJSONSerializer()
    """ The serializer to use when storing values."""

    def __init__(self, redis_instance: redis.Redis | None = None, key: str | None = None, max_age: int | None = None) -> None:
        """Constructor.

        Arguments:
            redis_instance (redis.Redis, optional): Redis instance; must be assigned before dictionary is accessed.
            key (string, optional): Redis hash key; if None a UUID will be assigned.
            max_age (int, optional): Redis hash key TTL in seconds, None if key does not expire.
        """
        self.redis = redis_instance
        self.key = key if key else self._generate_key()
        self.max_age = max_age

        if self.redis is not None:
            # Sanity check we have a Redis instance
            try:
                self.redis.pipeline()
            except AttributeError:
                errmsg = f"<{self!r}> redis instance is type <{self.redis.__class__.__name__}> expected type <Redis>"
                raise TypeError(errmsg) from None

    def __getitem__(self, name: str) -> RedisDictValuesT:
        """Return the value of field ``name``.

        Args:
            name (str): Field name.

        Raises:
            KeyError: Field does not exist.

        Returns:
            RedisDictValuesT
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        value = self.redis.hget(self.key, name)
        if value is None:
            raise KeyError(name)

        return self._loads(value)

    def __setitem__(self, name: str, value: RedisDictValuesT) -> None:
        """Set field ``name`` to ``value``.

        Resets hash key TTL to `max_age`.

        Args:
            name (str): Field name.
            value (RedisDictValuesT): Field value.
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        p = self.redis.pipeline()
        self._hset(p, name, value)
        if self.max_age is not None:
            p.expire(self.key, self.max_age)
        p.execute()

    def __delitem__(self, name: str) -> None:
        """Delete field ``name``.

        Resets hash key TTL to `max_age`.

        Args:
            name (str): Field name.
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        p = self.redis.pipeline()
        p.hdel(self.key, name)
        if self.max_age is not None:
            p.expire(self.key, self.max_age)
        p.execute()

    def __len__(self) -> int:
        """Return number of fields.

        Returns:
            int
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        return self.redis.hlen(self.key)

    def __repr__(self) -> str:
        """Return representation of instance.

        Returns:
            str
        """
        return f"{self.__class__.__name__}(key='{self.key}', max_age={self.max_age})"

    def __iter__(self) -> Iterator[str]:
        """Iterate field names.

        Yields:
            Iterator[str]
        """
        yield from self.keys()

    def __contains__(self, key: str) -> bool:
        """Return a boolean indicating whether field ``name`` exists.

        Args:
            key (str): Field name.

        Returns:
            bool
        """
        return self.has_key(key)

    def has_key(self, name: str) -> bool:
        """Return a boolean indicating whether field ``name`` exists.

        Args:
            name (str): Field name.

        Returns:
            bool
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        return self.redis.hexists(self.key, name)

    def keys(self) -> list[str]:
        """Return field names.

        Returns:
            list[str]
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        return self.redis.hkeys(self.key)

    def values(self) -> list[RedisDictValuesT]:
        """Return field values.

        Returns:
            list[RedisDictValuesT]
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        return [self._loads(v) for v in self.redis.hvals(self.key)]

    def items(self) -> list[tuple[str, RedisDictValuesT]]:
        """Return tuple (key, value) for all fields.

        Returns:
            list[tuple[str, RedisDictValuesT]]
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        return [(k, self._loads(v)) for k, v in self.redis.hgetall(self.key).items()]

    def delete(self) -> None:
        """Delete entire hash."""
        if self.redis is None:
            raise RedisDictNoRedisError

        if self.key:
            self._check_state()
            self.redis.delete(self.key)

    def update(self, other: Mapping[str, RedisDictValuesT] | Sequence[tuple[str, RedisDictValuesT]] | None = None, **kwargs) -> None:
        """Set values for multiple fields efficiently.

        Resets hash key TTL to `max_age`.

        Args:
            other (Mapping | Sequence[tuple[str, RedisDictValuesT]] | None, optional): Mapping or sequence of tuples.
            kwargs (dict): Key/value pairs as arguments.
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        if other is not None:
            self._check_state()
            p = self.redis.pipeline()
            if isinstance(other, Mapping):
                for k, v in other.items():
                    self._hset(p, k, v)
            else:
                for k, v in other:
                    self._hset(p, k, v)
            if self.max_age is not None:
                p.expire(self.key, self.max_age)
            p.execute()

        if kwargs:
            self.update(kwargs)

    def del_keys(self, fields: Collection[str], delay_execute: bool = False) -> redis.Pipeline | None:  # pyright:ignore[reportAttributeAccessIssue]
        """Delete multiple fields efficiently.

        Resets hash key TTL to `max_age`.

        Args:
            fields (Collection[str]): Collection of field names.
            delay_execute (bool, optional): True to delay pipeline execution. Defaults to False.

        Returns:
            redis.Pipeline if delayed, otherwise None.
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        self._check_state()

        if fields:
            p = self.redis.pipeline()
            for name in fields:
                p.hdel(self.key, name)
            if self.max_age is not None:
                p.expire(self.key, self.max_age)
            if delay_execute is True:
                return p
            p.execute()

        return None

    def exists(self) -> bool:
        """Return True if hash key exists.

        Returns:
            bool
        """
        if self.redis is None:
            raise RedisDictNoRedisError

        return bool(self.key) and bool(self.redis.exists(self.key))

    def _check_state(self) -> None:
        """Asserts internal state is safe to access.

        Raises:
            ValueError: Redis instance has not been set.
            TypeError: Redis instance is not of type `Redis`.
        """

    def _generate_key(self) -> str:
        """Generate a hash key."""
        return str(uuid4())

    def _dumps(self, value: RedisDictValuesT) -> str:
        """Serialize ``value``."""
        if self.serializer is not None:
            return self.serializer.dumps(value)
        return value

    def _loads(self, value: str) -> RedisDictValuesT:
        """Unserialize ``value``."""
        if value is not None and self.serializer is not None:
            return self.serializer.loads(value)
        return value

    def _hset(self, p: redis.Pipeline, field: str, value: RedisDictValuesT, key: str | None = None) -> None:  # pyright:ignore[reportAttributeAccessIssue]
        """Helper function to serialize hset values."""
        key = key or self.key
        p.hset(key, field, self._dumps(value))
