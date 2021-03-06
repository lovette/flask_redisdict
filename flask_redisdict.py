# -*- coding: utf-8 -*-

from flask.sessions import TaggedJSONSerializer
from UserDict import DictMixin
from uuid import uuid4
import redis


class RedisDict(object, DictMixin):
    """Acts like a dictionary but reflects item access to Redis.

    Attributes:
        redis (redis.StrictRedis): Redis instance.
        key (string): Hash key.
        max_age (int): Hash key TTL in seconds, None if key does not expire.

    Note:
        Hash key TTL is updated whenever values are updated or removed.
    """

    serializer = TaggedJSONSerializer()
    """ The serializer to use when storing values."""

    def __init__(self, redis_instance=None, key=None, max_age=None):
        """Constructor

        Arguments:
            redis_instance (StrictRedis): Redis instance.
            key (string): Dict hash key.
            max_age (int): TTL of dict hash, in seconds. None if key does not expire.
        """
        self.redis = redis_instance
        self.key = key
        self.max_age = max_age

    def __getitem__(self, name):
        """Return the value of hash field ``name``, raises a KeyError if the field doesn't exist."""
        self._check_state()
        value = self.redis.hget(self.key, name)
        if value is None:
            raise KeyError(name)
        return self._loads(value)

    def __setitem__(self, name, value):
        """Sets hash field ``name`` to ``value``."""
        self._check_state()
        p = self.redis.pipeline()
        self._hset(p, name, value)
        if self.max_age is not None:
            p.expire(self.key, self.max_age)
        p.execute()

    def __delitem__(self, name):
        """Deletes hash field ``name``."""
        self._check_state()
        p = self.redis.pipeline()
        p.hdel(self.key, name)
        if self.max_age is not None:
            p.expire(self.key, self.max_age)
        p.execute()

    def __len__(self):
        """Returns number of hash fields in hash."""
        self._check_state()
        return self.redis.hlen(self.key)

    def __repr__(self):
        """Return representation of instance."""
        return "%s(key='%s', max_age=%d)" % (self.__class__.__name__, self.key, self.max_age)

    def has_key(self, name):
        """Returns a boolean indicating whether hash field ``name`` exists."""
        self._check_state()
        return self.redis.hexists(self.key, name)

    def keys(self):
        """Returns hash fields in hash."""
        self._check_state()
        return self.redis.hkeys(self.key)

    def values(self):
        """Returns hash field values in hash."""
        self._check_state()
        return [self._loads(v) for v in self.redis.hvals(self.key)]

    def items(self):
        """Returns tuple (key, value) for all hash fields in hash."""
        self._check_state()
        return [(k, self._loads(v)) for k, v in self.redis.hgetall(self.key).items()]

    def delete(self):
        """Deletes entire hash and resets key."""
        if self.key is not None:
            self._check_state()
            self.redis.delete(self.key)
            self.key = None

    def update(self, other=None, **kwargs):
        """Efficient way to set multiple hash fields."""

        # Same logic as UserDict.update
        if other is not None:
            self._check_state()
            p = self.redis.pipeline()
            if hasattr(other, 'iteritems'):
                for k, v in other.iteritems():
                    self._hset(p, k, v)
            elif hasattr(other, 'keys'):
                for k in other.keys():
                    self._hset(p, k, other[k])
            else:
                for k, v in other:
                    self._hset(p, k, v)
            if self.max_age is not None:
                p.expire(self.key, self.max_age)
            p.execute()

        if kwargs:
            self.update(kwargs)

    def del_keys(self, fields, delay_execute=False):
        """Efficient way to delete multiple hash fields from hash."""
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

    def exists(self):
        """Returns True if hash key exists."""
        if self.key is None:
            return False
        if self.redis.exists(self.key):
            return True
        self.key = None  # hash is invalid and should be recreated
        return False

    def _check_state(self):
        """Asserts internal state is safe to access."""
        if self.redis is None:
            raise AttributeError('<%r> has no redis instance' % self)
        assert isinstance(self.redis, redis.StrictRedis)
        if self.key is None:
            self.key = self._create_hash()

    def _create_hash(self):
        """Generate a new hash key and create the hash."""
        return self._generate_key()

    def _generate_key(self):
        """Generate a hash key."""
        return str(uuid4())

    def _dumps(self, value):
        """Serialize ``value``."""
        if self.serializer is not None:
            return self.serializer.dumps(value)
        return value

    def _loads(self, value):
        """Unserialize ``value``."""
        if value is not None and self.serializer is not None:
            return self.serializer.loads(value)
        return value

    def _hset(self, p, field, value, key=None):
        """Helper function to serialize hset values."""
        assert p is not None
        key = key or self.key
        p.hset(key, field, self._dumps(value))
