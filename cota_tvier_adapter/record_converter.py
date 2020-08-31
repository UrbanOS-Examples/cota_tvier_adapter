"""
Converts TVIER records to the standard SCOS CVE schema
"""
from xml.etree.ElementTree import fromstring

from xmljson import parker

event_map = {"receivedBSM": "BSM", "sentBSM": "BSM"}


def convert_row(rows, row):
    """
    Maps the TVIER format to SCOS
    """
    event_type = row["EventType"]

    obj = {}
    obj["timestamp"] = row["Timestamp"]
    if event_type in event_map.keys():
        (message_body, event_type) = _convert_known_event(row)
    else:
        (message_body, event_type) = _convert_unknown_event(row)

    obj["messageBody"] = message_body
    obj["messageType"] = event_type

    rows.append(obj)
    return rows


def _convert_known_event(event):
    event_data = _convert_body(event["EventData"])
    message_body = list(event_data["message"]["MessageFrame"]["value"].values())[0]
    event_type = event_map[event["EventType"]]
    return (message_body, event_type)


def _convert_unknown_event(event):
    message_body = _convert_body(event["EventData"])
    event_type = event["EventType"]
    return (message_body, event_type)


def _convert_body(xml):
    return parker.data(fromstring(xml))
