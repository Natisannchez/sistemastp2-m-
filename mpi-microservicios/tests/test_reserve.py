from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
import time

import pytest
import redis
from fastapi.testclient import TestClient

from catalogo.main import PRODUCTOS, app, get_redis_client


class FakeRedisLock:
    def __init__(self):
        self._store = {}
        self._lock = Lock()

    def set(self, key, value, nx=False, ex=None):
        with self._lock:
            if nx and key in self._store:
                return False
            self._store[key] = value
            return True

    def eval(self, script, numkeys, key, token):
        with self._lock:
            if self._store.get(key) == token:
                del self._store[key]
                return 1
            return 0


class FailingRedis:
    def set(self, key, value, nx=False, ex=None):
        raise redis.TimeoutError("redis timeout")

    def eval(self, script, numkeys, key, token):
        return 0


def _reset_stock(sku="SKU-001", stock=1):
    PRODUCTOS[sku]["stock"] = stock


@pytest.fixture(autouse=True)
def _cleanup_overrides():
    yield
    app.dependency_overrides.clear()


def test_reserve_two_users_same_product():
    _reset_stock("SKU-001", 1)
    fake_redis = FakeRedisLock()
    app.dependency_overrides[get_redis_client] = lambda: fake_redis

    client = TestClient(app)
    barrier = Barrier(2)

    def buy_once():
        barrier.wait()
        return client.post("/reserve", json={"sku": "SKU-001", "cantidad": 1}).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: buy_once(), range(2)))

    success = sum(1 for code in results if code == 200)
    failures = sum(1 for code in results if code in (400, 503))

    assert success == 1
    assert failures == 1
    assert PRODUCTOS["SKU-001"]["stock"] == 0
    assert PRODUCTOS["SKU-001"]["stock"] >= 0


def test_reserve_fifty_users_ten_products():
    _reset_stock("SKU-001", 10)
    fake_redis = FakeRedisLock()
    app.dependency_overrides[get_redis_client] = lambda: fake_redis

    client = TestClient(app)
    barrier = Barrier(50)

    def buy_with_retry(max_retries=100):
        barrier.wait()
        for _ in range(max_retries):
            resp = client.post("/reserve", json={"sku": "SKU-001", "cantidad": 1})
            if resp.status_code == 503:
                time.sleep(0.002)
                continue
            return resp.status_code
        return 503

    with ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(lambda _: buy_with_retry(), range(50)))

    success = sum(1 for code in results if code == 200)
    no_stock = sum(1 for code in results if code == 400)

    assert success == 10
    assert no_stock == 40
    assert PRODUCTOS["SKU-001"]["stock"] == 0
    assert PRODUCTOS["SKU-001"]["stock"] >= 0


def test_reserve_redis_timeout_fails_fast():
    _reset_stock("SKU-001", 5)
    app.dependency_overrides[get_redis_client] = lambda: FailingRedis()

    client = TestClient(app)

    start = time.perf_counter()
    response = client.post("/reserve", json={"sku": "SKU-001", "cantidad": 1})
    elapsed = time.perf_counter() - start

    assert response.status_code == 503
    assert elapsed < 0.5

