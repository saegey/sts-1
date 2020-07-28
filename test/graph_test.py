import json
from test.mocks.dynamo import peaks_table, strava_auth_table

from moto import mock_dynamodb2

from functions.graph import main
from lib.config import Config

config = Config()


def test_empty_athlete_graph():
    event = {"requestContext": {"authorizer": {"athleteId": None}}}

    val = main(event, {})
    assert val == {
        "body": "[]",
        "headers": {
            "Access-Control-Allow-Credentials": True,
            "Access-Control-Allow-Origin": "*",
        },
        "statusCode": 200,
    }


@mock_dynamodb2
def test_valid_athlete_graph():
    event = {
        "requestContext": {
            "authorizer": {"athleteId": "1234", "prb incipalId": "420"}
        },
        "queryStringParameters": {
            "type": "Ride",
            "attribute": "watts",
            "duration": 500,
        },
    }

    peak_item = {
        "activity_id": "765275065",
        "athlete_id": "1234",
        "name": "Zwift - London, UK - FTP Test",
        "peak_id": "765275065_watts_1200",
        "peak_type": "Ride_watts_500",
        "start_date_local": "2016-11-03T21:05:26",
        "value": "272.4483333333333",
    }

    peaks_tbl = peaks_table()
    peaks_tbl.put_item(Item=peak_item)

    val = main(event, {})
    # print(val)
    peak_item["start_date_local"] = "2016-11-03 21:05:26"
    assert val == {
        "body": json.dumps([peak_item], default=str),
        "headers": {
            "Access-Control-Allow-Credentials": True,
            "Access-Control-Allow-Origin": "*",
        },
        "statusCode": 200,
    }
