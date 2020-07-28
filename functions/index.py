import json

import boto3
from boto3.dynamodb.conditions import Key
from stravalib.client import Client as StravaClient

from lib.config import Config
from lib.activity_peak import ActivityPeak
from lib.recent_athlete_peak import RecentAthletePeak
from lib.strava_activity import StravaActivity

config = Config()
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
strava_auth_table = dynamodb.Table(config.strava_auth_table)
sqs = boto3.client("sqs")


def backfill_athlete(event):
    user_id = event["requestContext"]["authorizer"]["principalId"]
    auth_response = strava_auth_table.get_item(Key={"user_id": user_id})
    # TODO - if no auth response it should return error

    response = sqs.send_message(
        QueueUrl=config.backfill_athlete_queue,
        DelaySeconds=0,
        MessageAttributes={
            "Job": {"DataType": "String", "StringValue": "BACKFILL_ATHLETE"},
            "UserId": {"DataType": "String", "StringValue": user_id},
            "AthleteId": {
                "DataType": "String",
                "StringValue": auth_response["Item"]["athlete_id"],
            },
        },
        MessageBody=(
            "Backfill athlete for user {user_id}".format(user_id=user_id)
        ),
    )
    print(response)
    return {"statusCode": 200, "body": json.dumps(response)}


def strava_authorized(event, context):
    payload = json.loads(event["body"])
    strava_client = StravaClient()
    code = payload["code"] or None

    token_response = strava_client.exchange_code_for_token(
        client_id=config.strava_client_id,
        client_secret=config.strava_client_secret,
        code=code,
    )
    access_token = token_response["access_token"]
    refresh_token = token_response["refresh_token"]
    expires_at = token_response["expires_at"]

    strava_client.access_token = access_token

    athlete = strava_client.get_athlete()
    user_id = event["requestContext"]["authorizer"]["principalId"]

    strava_auth_record = {
        "user_id": user_id,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "athlete_id": str(athlete.id),
    }
    response = strava_auth_table.put_item(Item=strava_auth_record)
    print("saved strava auth credentials", response)
    # TODO - if no auth response it should return error

    response = sqs.send_message(
        QueueUrl=config.backfill_athlete_queue,
        DelaySeconds=0,
        MessageAttributes={
            "Job": {"DataType": "String", "StringValue": "BACKFILL_ATHLETE"},
            "UserId": {"DataType": "String", "StringValue": user_id},
            "AthleteId": {
                "DataType": "String",
                "StringValue": str(athlete.id),
            },
        },
        MessageBody=(
            "Backfill athlete for user {user_id}".format(user_id=user_id)
        ),
    )
    print("enqueued strava backfill post authorization", response)
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "body": '{"strava_authorized": true}',
    }


def profile(event, context):
    user_id = event["requestContext"]["authorizer"]["principalId"]
    response = strava_auth_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    event["requestContext"]["authorizer"]["hasStravaAuth"] = (
        True if len(response["Items"]) else False
    )

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "body": json.dumps(event["requestContext"]["authorizer"]),
    }


def activity_peaks(event, context):
    athlete_id = event["requestContext"]["authorizer"]["athleteId"]
    activity_id = event["pathParameters"]["activityId"]
    res = ActivityPeak.get_athlete_from_s3(athlete_id)

    peaks = []
    for peak_type in res.keys():
        index = 1
        for peak in res[peak_type]:
            if peak["activity_id"] == activity_id:
                peak["rank"] = index
                peaks.append(peak)
            index += 1

    return {
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "statusCode": 200,
        "body": json.dumps(peaks, default=str),
    }


def activity(event, context):
    athlete_id = event["requestContext"]["authorizer"]["athleteId"]
    activity_id = event["pathParameters"]["activityId"]
    s3_activity = StravaActivity.getFromS3(athlete_id, activity_id)

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "body": json.dumps(s3_activity, default=str),
    }


def recent_peaks(event, context):
    user_id = event["requestContext"]["authorizer"]["principalId"]
    response = strava_auth_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    if len(response["Items"]) == 0:
        return {
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
            },
            "statusCode": 200,
            "body": json.dumps([], default=str),
        }

    athlete_id = response["Items"][0]["athlete_id"]
    res = RecentAthletePeak.fetch(athlete_id)
    formatted_items = []
    for item in res["Items"]:
        formatted_items += item["data"]

    formatted_items.sort(key=lambda x: int(x["date_timestamp"]), reverse=True)

    return {
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "statusCode": 200,
        "body": json.dumps(formatted_items, default=str),
    }
