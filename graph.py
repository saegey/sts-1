import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import json
from pprint import pprint
from botocore.exceptions import ClientError

PEAKS_TABLE = os.environ["PEAKS_TABLE"]
STRAVA_AUTH_TABLE_NAME = os.environ["STRAVA_AUTH_TABLE"]

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
peaks_table = dynamodb.Table(PEAKS_TABLE)
strava_auth_table = dynamodb.Table(STRAVA_AUTH_TABLE_NAME)


def main(event, context):
    # print(event)
    user_id = event["requestContext"]["authorizer"]["principalId"]
    try:
        response = strava_auth_table.get_item(Key={"user_id": user_id})
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        params = {
            "limit": 10,
            "duration": 300,
            "attribute": "watts",
            "type": "Ride",
        }

        for p in params.keys():
            if event["queryStringParameters"] and p in event["queryStringParameters"]:
                params[p] = event["queryStringParameters"][p]

        athlete_id = response["Item"]["athlete_id"]
        peaks_type = "{type}_{attribute}_{duration}".format(
            type=params["type"],
            attribute=params["attribute"],
            duration=params["duration"]
        )
        print(athlete_id, peaks_type)
        peaks_response = peaks_table.query(
            IndexName='peaks_type',  # example key = Ride_velocity_smooth_300
            KeyConditionExpression=Key('athlete_id').eq(
                athlete_id) & Key('peak_type').eq(peaks_type),
            Select='ALL_PROJECTED_ATTRIBUTES',
            ReturnConsumedCapacity='INDEXES',
            ScanIndexForward=False,  # return results in descending order of sort key
        )
        peaks = peaks_response["Items"]

        max_limit = (
            int(params["limit"]) if int(
                params["limit"]) <= len(peaks) else len(peaks)
        )

        for peak in peaks:
            if isinstance(peak["start_date_local"], datetime.datetime):
                continue

            peak["start_date_local"] = datetime.datetime.strptime(
                peak["start_date_local"], "%Y-%m-%dT%H:%M:%S"
            )

            if params["attribute"] == "velocity_smooth":
                peak["value"] = 2.23694 * float(peak["value"])
                # peak["converted"] = True

        sorted_filter_peaks = sorted(peaks, key=lambda i: i["value"], reverse=True)[
            0: int(max_limit)
        ]

        return {
            "statusCode": 200,
            "body": json.dumps(sorted_filter_peaks, default=str),
        }
