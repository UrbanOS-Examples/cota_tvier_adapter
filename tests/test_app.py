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
    fixture_date = "2020_11_15"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.gz")

    assert response.status_code == 200
    print(response.text[0:100])
    assert {
        "timestamp": "11/14/2020 22:16:34.009",
        "messageBody": {
            "coreData": {
                "msgCnt": 11,
                "id": "91F8EB3E",
                "secMark": 33950,
                "lat": 399057058,
                "long": -828245967,
                "elev": 2018,
                "accuracy": {"semiMajor": 8, "semiMinor": 7, "orientation": 31437},
                "transmission": {"unavailable": None},
                "speed": 90,
                "heading": 22377,
                "angle": 127,
                "accelSet": {"long": 97, "lat": 69, "vert": 49, "yaw": 531},
                "brakes": {
                    "wheelBrakes": 10000,
                    "traction": {"unavailable": None},
                    "abs": {"unavailable": None},
                    "scs": {"unavailable": None},
                    "brakeBoost": {"off": None},
                    "auxBrakes": {"unavailable": None},
                },
                "size": {"width": 310, "length": 1240},
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
                                            "latOffset": 570,
                                            "lonOffset": -9634,
                                            "elevationOffset": -3,
                                            "timeOffset": 8370,
                                        },
                                        {
                                            "latOffset": 587,
                                            "lonOffset": -11188,
                                            "elevationOffset": -4,
                                            "timeOffset": 8835,
                                        },
                                        {
                                            "latOffset": 90,
                                            "lonOffset": -11831,
                                            "elevationOffset": -3,
                                            "timeOffset": 9205,
                                        },
                                        {
                                            "latOffset": -4072,
                                            "lonOffset": -12234,
                                            "elevationOffset": 2,
                                            "timeOffset": 10705,
                                        },
                                        {
                                            "latOffset": -8491,
                                            "lonOffset": -12820,
                                            "elevationOffset": 0,
                                            "timeOffset": 11655,
                                        },
                                        {
                                            "latOffset": -9859,
                                            "lonOffset": -13344,
                                            "elevationOffset": -4,
                                            "timeOffset": 12075,
                                        },
                                        {
                                            "latOffset": -10427,
                                            "lonOffset": -14296,
                                            "elevationOffset": -3,
                                            "timeOffset": 12405,
                                        },
                                        {
                                            "latOffset": -10541,
                                            "lonOffset": -17546,
                                            "elevationOffset": -8,
                                            "timeOffset": 13365,
                                        },
                                        {
                                            "latOffset": -11016,
                                            "lonOffset": -19052,
                                            "elevationOffset": -7,
                                            "timeOffset": 13755,
                                        },
                                        {
                                            "latOffset": -12383,
                                            "lonOffset": -20414,
                                            "elevationOffset": -7,
                                            "timeOffset": 14305,
                                        },
                                    ]
                                }
                            },
                            "pathPrediction": {"radiusOfCurve": 357, "confidence": 100},
                            "lights": 100000,
                        }
                    },
                }
            },
        },
        "messageType": "BSM",
        "sourceDevice": "2520203122717008045",
    } in response.json()
