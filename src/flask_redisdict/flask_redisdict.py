"""Flask extension that allows access to Redis hash as a dictionary.

This module provides:
- RedisDict
"""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping, Sequence
from typing import TYPE_CHECKING
from uuid import uuid4

import redis
from flask.sessions import TaggedJSONSerializer

if TYPE_CHECKING:
    from collections.abc import Mapping


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

    def __getitem__(self, name: str) -> str | int | dict | Sequence:
        """Return the value of field ``name``.

        Args:
            name (str): Field name.

        Raises:
            KeyError: Field does not exist.

        Returns:
            str | int | dict | Sequence
        """
        self._check_state()
        value = self.redis.hget(self.key, name)
        if value is None:
            raise KeyError(name)
        return self._loads(value)

    def __setitem__(self, name: str, value: str | int | dict | Sequence) -> None:
        """Set field ``name`` to ``value``.

        Resets hash key TTL to `max_age`.

        Args:
            name (str): Field name.
            value (str | int | dict | Sequence): Field value.
        """
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
        self._check_state()
        return self.redis.hexists(self.key, name)

    def keys(self) -> list[str]:
        """Return field names.

        Returns:
            list[str]
        """
        self._check_state()
        return self.redis.hkeys(self.key)

    def values(self) -> list[str | int | dict | Sequence]:
        """Return field values.

        Returns:
            list[str | int | dict | Sequence]
        """
        self._check_state()
        return [self._loads(v) for v in self.redis.hvals(self.key)]

    def items(self) -> list[tuple[str, str | int | dict | Sequence]]:
        """Return tuple (key, value) for all fields.

        Returns:
            list[tuple[str, str | int | dict | Sequence]]
        """
        self._check_state()
        return [(k, self._loads(v)) for k, v in self.redis.hgetall(self.key).items()]

    def delete(self) -> None:
        """Delete entire hash."""
        if self.key:
            self._check_state()
            self.redis.delete(self.key)

    def update(self, other: dict | Mapping | list[tuple[str, str | int | dict | Sequence]] | None = None, **kwargs) -> None:
        """Set values for multiple fields efficiently.

        Resets hash key TTL to `max_age`.

        Args:
            other (dict | Mapping | list[tuple[str, str  |  int  |  dict  |  Sequence]] | None, optional): Dictionary, mapping or sequence of tuples.
            kwargs (dict): Key/value pairs as arguments.
        """
        # Same logic as UserDict.update
        if other is not None:
            self._check_state()
            p = self.redis.pipeline()
            if hasattr(other, "items"):
                for k, v in other.items():
                    self._hset(p, k, v)
            elif hasattr(other, "keys"):
                for k in other:
                    self._hset(p, k, other[k])
            else:
                for k, v in other:
                    self._hset(p, k, v)
            if self.max_age is not None:
                p.expire(self.key, self.max_age)
            p.execute()

        if kwargs:
            self.update(kwargs)

    def del_keys(self, fields: Sequence[str], delay_execute: bool = False) -> redis.Pipeline | None:
        """Delete multiple fields efficiently.

        Resets hash key TTL to `max_age`.

        Args:
            fields (Sequence[str]): Sequence of field names.
            delay_execute (bool, optional): True to delay pipeline execution. Defaults to False.

        Returns:
            redis.Pipeline if delayed, otherwise None.
        """
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
        return self.key and self.redis.exists(self.key)

    def _check_state(self) -> None:
        """Asserts internal state is safe to access.

        Raises:
            ValueError: Redis instance has not been set.
            TypeError: Redis instance is not of type `Redis`.
        """
        if self.redis is None:
            errmsg = f"<{self!r}> has no redis instance"
            raise ValueError(errmsg)
        if not isinstance(self.redis, redis.Redis):
            errmsg = f"<{self!r}> redis instance is type <{self.redis.__class__.__name__}> expected type <Redis>"
            raise TypeError(errmsg)

    def _generate_key(self) -> str:
        """Generate a hash key."""
        return str(uuid4())

    def _dumps(self, value: str | int | dict | Sequence) -> str:
        """Serialize ``value``."""
        if self.serializer is not None:
            return self.serializer.dumps(value)
        return value

    def _loads(self, value: str) -> str | int | dict | Sequence:
        """Unserialize ``value``."""
        if value is not None and self.serializer is not None:
            return self.serializer.loads(value)
        return value

    def _hset(self, p: redis.Pipeline, field: str, value: str | int | dict | Sequence, key: str | None = None) -> None:
        """Helper function to serialize hset values."""
        key = key or self.key
        p.hset(key, field, self._dumps(value))
