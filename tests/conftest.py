"""Define fixtures accessible to all tests."""

import pytest
from fakeredis import FakeRedis


@pytest.fixture
def redis_client() -> FakeRedis:
    return FakeRedis()
