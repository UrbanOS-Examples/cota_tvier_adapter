import json

import pytest
from fastapi.testclient import TestClient

from cota_tvier_adapter import app
from tests.fake_cota_content_server import create_fake_server

client = TestClient(app)


@pytest.fixture(scope="module")
def fake_content_server_url():
    return create_fake_server()


def test_high_level(fake_content_server_url):
    fixture_url = fake_content_server_url + "/cota_obu_data"
    fixture_date = "2020_07_22"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.tgz")

    assert response.status_code == 200
    assert {
        "timestamp": "07/22/2020 09:20:41.653",
        "sourceDevice": "2520203092017006460",
        "messageType": "BSM",
        "messageBody": {
            "coreData": {
                "msgCnt": 13,
                "id": "090001BC",
                "secMark": 41422,
                "lat": 265502004,
                "long": -800923731,
                "elev": -217,
                "accuracy": {"semiMajor": 6, "semiMinor": 5, "orientation": 29526},
                "transmission": {"unavailable": None},
                "speed": 921,
                "heading": 16299,
                "angle": 127,
                "accelSet": {"long": 3, "lat": -3, "vert": -50, "yaw": -22},
                "brakes": {
                    "wheelBrakes": 10000,
                    "traction": {"unavailable": None},
                    "abs": {"unavailable": None},
                    "scs": {"unavailable": None},
                    "brakeBoost": {"off": None},
                    "auxBrakes": {"unavailable": None},
                },
                "size": {"width": 180, "length": 450},
            },
            "partII": {
                "SEQUENCE": {
                    "partII-Id": 0,
                    "partII-Value": {
                        "VehicleSafetyExtensions": {
                            "pathHistory": {
                                "crumbData": {
                                    "PathHistoryPoint": [
                                        {
                                            "latOffset": -754,
                                            "lonOffset": -367,
                                            "elevationOffset": 0,
                                            "timeOffset": 50,
                                        },
                                        {
                                            "latOffset": -17946,
                                            "lonOffset": -8676,
                                            "elevationOffset": -2,
                                            "timeOffset": 1190,
                                        },
                                    ]
                                }
                            },
                            "pathPrediction": {
                                "radiusOfCurve": 32767,
                                "confidence": 200,
                            },
                        }
                    },
                }
            },
        },
    } in response.json()
