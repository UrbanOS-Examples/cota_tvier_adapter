import pytest
from fastapi.testclient import TestClient

from cota_tvier_adapter import app

client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}