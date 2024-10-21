"""RedisDict tests."""

from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from fakeredis import FakeRedis

from flask_redisdict import RedisDict

SESSION_KEY = "pytest_session_key"


def test_redis_instance_none(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict()
    with pytest.raises(ValueError, match="has no redis instance"):
        redis_dict["A"] = "ValueA"


def test_redis_instance_not_redis(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(object())
    with pytest.raises(TypeError, match="expected type <Redis>"):
        redis_dict["A"] = "ValueA"


def test_not_exists(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)

    assert not redis_client.keys()
    assert not redis_dict.exists()


def test_exists_uuidkey(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client)
    redis_dict["A"] = "ValueA"

    assert redis_dict.exists()
    assert len(redis_client.keys()) == 1


def test_exists(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"

    assert redis_dict.exists()
    assert redis_client.keys() == [SESSION_KEY.encode()]  # encode->bytes


def test_exists_multi(redis_client: FakeRedis) -> None:
    redis_dict1 = RedisDict(redis_client, "session_key_1")
    redis_dict1["A"] = "ValueA"

    redis_dict2 = RedisDict(redis_client, "session_key_2")
    redis_dict2["Z"] = "ValueA"

    assert redis_dict1.exists()
    assert redis_dict2.exists()
    assert redis_client.keys() == [b"session_key_1", b"session_key_2"]


def test_delete(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    assert not redis_client.keys()

    redis_dict["A"] = "ValueA"

    assert redis_dict.exists()
    assert redis_client.keys() == [SESSION_KEY.encode()]  # encode->bytes
    assert len(redis_client.keys()) == 1

    redis_dict.delete()

    assert not redis_dict.exists()
    assert not redis_client.keys()
    assert len(redis_client.keys()) == 0


def test_set_a_str(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    val = "ValueA"
    redis_dict["A"] = val

    assert isinstance(redis_dict["A"], str)
    assert redis_dict["A"] == val
    assert len(redis_client.keys()) == 1


def test_set_a_int(redis_client: FakeRedis) -> None:
    val = 999
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = val

    assert isinstance(redis_dict["A"], int)
    assert redis_dict["A"] == val
    assert len(redis_client.keys()) == 1


def test_set_a_dict(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    val = {"AA": 1, "BB": 2}
    redis_dict["A"] = val

    assert isinstance(redis_dict["A"], dict)
    assert redis_dict["A"] == val
    assert len(redis_client.keys()) == 1


def test_set_ab(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert redis_dict["A"] == "ValueA"
    assert redis_dict["B"] == "ValueB"
    assert len(redis_client.keys()) == 1


def test_del(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert len(redis_dict) == 2

    del redis_dict["B"]

    assert len(redis_dict) == 1


def test_del_keys(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert len(redis_dict) == 2

    redis_dict.del_keys(("A", "B"))

    assert len(redis_dict) == 0


def test_update_dict(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)

    redis_dict.update(
        {
            "A": "ValueA",
            "B": "ValueB",
        },
    )

    assert redis_dict["A"] == "ValueA"
    assert redis_dict["B"] == "ValueB"
    assert len(redis_client.keys()) == 1


def test_update_kwargs(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)

    redis_dict.update(
        A="ValueA",
        B="ValueB",
    )

    assert redis_dict["A"] == "ValueA"
    assert redis_dict["B"] == "ValueB"
    assert len(redis_client.keys()) == 1


def test_update_tuples(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)

    redis_dict.update(
        (
            ("A", "ValueA"),
            ("B", "ValueB"),
        ),
    )

    assert redis_dict["A"] == "ValueA"
    assert redis_dict["B"] == "ValueB"
    assert len(redis_client.keys()) == 1


def test_clear(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert len(redis_dict) == 2

    redis_dict.clear()

    assert len(redis_dict) == 0


def test_keyerror_get(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"

    assert len(redis_dict) == 1

    with pytest.raises(KeyError, match="B"):
        redis_dict["B"] == "ValueB" or True


def test_keyerror_del(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"

    assert len(redis_dict) == 1

    with does_not_raise():
        # Should NOT raise a KeyError
        del redis_dict["B"]


def test_keys(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert redis_dict.keys() == [b"A", b"B"]
    assert len(redis_client.keys()) == 1


def test_values(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"

    assert redis_dict.values() == ["ValueA", "ValueB"]
    assert len(redis_client.keys()) == 1


def test_in(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"

    assert "A" in redis_dict
    assert len(redis_client.keys()) == 1


def test_not_in(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"

    assert "B" not in redis_dict
    assert len(redis_client.keys()) == 1


def test_len_0(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)

    assert len(redis_dict) == 0
    assert len(redis_client.keys()) == 0


def test_len_3(redis_client: FakeRedis) -> None:
    redis_dict = RedisDict(redis_client, SESSION_KEY)
    redis_dict["A"] = "ValueA"
    redis_dict["B"] = "ValueB"
    redis_dict["C"] = "ValueC"

    assert len(redis_dict) == 3
    assert len(redis_client.keys()) == 1
