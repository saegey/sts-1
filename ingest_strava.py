import boto3
import os
import io
from stravalib.client import Client as StravaClient
import requests
from datetime import datetime, timedelta
import json
import time
from pprint import pprint
from ratelimiter import RateLimiter
from config import Config

STREAM_TYPES = [
    "time",
    "distance",
    "latlng",
    "altitude",
    "grade_smooth",
    "velocity_smooth",
    "heartrate",
    "watts",
]
strava_client = StravaClient()
config = Config()

s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
strava_auth_table = dynamodb.Table(config.strava_auth_table)

def new_token(athlete_id, refresh_token):
    raw_res = requests.post(
        "{url}/oauth/token".format(url=config.strava_api_uri),
        data={
            "client_id": config.strava_client_id,
            "client_secret": config.strava_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )
    res = raw_res.json()
    return {
        "refresh_token": res["refresh_token"],
        "access_token": res["access_token"],
        "expires_at": res["expires_at"],
    }


def get_and_save_strava_activity(strava_client, athlete_id, user_id, before, after):
    activities = strava_client.get_activities(
        before=before, after=after, limit=50
    )
    for activity in activities:
        activity_filename = "activity_{athlete_id}_{activity_id}.json".format(
            athlete_id=athlete_id, activity_id=activity.id
        )
        s3_client.put_object(
            Body=json.dumps(activity.to_dict(), sort_keys=True, default=str),
            Bucket=config.strava_api_s3_bucket,
            Key=activity_filename,
        )
        print('saving activity to {bucket}:{key}'.format(
            bucket=config.strava_api_s3_bucket, key=activity_filename))

        response = sqs.send_message(
            QueueUrl=config.strava_api_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                "Job": {"DataType": "String", "StringValue": "FETCH_STRAVA_STREAM"},
                "AthleteId": {"DataType": "String", "StringValue": str(athlete_id)},
                "UserId": {"DataType": "String", "StringValue": str(user_id)},
                "ActivityId": {"DataType": "String", "StringValue": str(activity.id)},
                "AccessToken": {"DataType": "String", "StringValue": strava_client.access_token},
            },
            MessageBody=(
                "Get strava athlete stream for {athlete_id} for activity {activity_id}".format(
                    athlete_id=athlete_id,
                    activity_id=activity.id
                )
            ),
            MessageGroupId="STRAVA-API"
        )
        print(response)


def get_and_save_strava_streams(strava_client, activity_id, athlete_id):
    streams_filename = "streams_{athlete_id}_{activity_id}.json".format(
        athlete_id=athlete_id, activity_id=activity_id)
    streams = strava_client.get_activity_streams(
        activity_id, types=STREAM_TYPES
    )
    formatted_streams = {}
    if streams is None:
        return False

    for stream in streams.keys():
        formatted_streams[stream] = streams[stream].data

    s3_client.put_object(
        Body=json.dumps(formatted_streams, default=str),
        Bucket=config.strava_api_s3_bucket,
        Key=streams_filename,
    )
    print('saving streams to {bucket}:{key}'.format(
        bucket=config.strava_api_s3_bucket, key=streams_filename))
    return True


@RateLimiter(max_calls=600, period=900)
def strava_api_call(job_type, message_attribs, athlete_id, user_id):
    if job_type == "FETCH_STRAVA_ACTIVITY":
        before = datetime.strptime(
            message_attribs['BeforeDate']['stringValue'], "%m/%d/%Y")
        after = datetime.strptime(
            message_attribs['AfterDate']['stringValue'], "%m/%d/%Y")
        get_and_save_strava_activity(
            strava_client=strava_client,
            athlete_id=athlete_id,
            user_id=user_id,
            before=before,
            after=after)
    elif job_type == "FETCH_STRAVA_STREAM":
        activity_id = message_attribs['ActivityId']['stringValue']
        get_and_save_strava_streams(
            strava_client=strava_client, athlete_id=athlete_id, activity_id=activity_id)


def enqueue_strava_athlete_sync(event, context):
    print(event, config)
    return True


def sns_retry_strava_api(event, context):
    pprint(json.loads(event['Sns']['Message']))


def fetch_strava_api(event, context):
    for record in event['Records']:
        print(record)
        message_attribs = record['messageAttributes']
        job_type = message_attribs['Job']['stringValue']
        athlete_id = message_attribs['AthleteId']['stringValue']
        user_id = message_attribs['UserId']['stringValue']

        strava_client.access_token = athlete_access_token(user_id)
        strava_api_call(job_type=job_type, message_attribs=message_attribs,
                        athlete_id=athlete_id, user_id=user_id)


def athlete_access_token(user_id):
    res = strava_auth_table.get_item(Key={"user_id": user_id})
    expires_at = res["Item"]["expires_at"]
    athlete_id = res["Item"]["athlete_id"]
    refresh_token = res["Item"]["refresh_token"]
    access_token = None

    if datetime.now().timestamp() > expires_at:
        token = new_token(athlete_id, refresh_token)
        print("new token returned: {token}".format(token=str(token)))
        print("updating token for {athlete_id}".format(athlete_id=athlete_id))
        token_update_res = strava_auth_table.put_item(
            Item={
                "user_id": res["Item"]["user_id"],
                "access_token": token["access_token"],
                "refresh_token": token["refresh_token"],
                "expires_at": token["expires_at"],
                "athlete_id": str(athlete_id),
            }
        )
        print(token_update_res)
        access_token = token["access_token"]
    else:
        print('token is valid for {athlete_id}'.format(athlete_id=athlete_id))
        access_token = res["Item"]['access_token']

    return access_token


def enqueue_strava_backfill(event, context):
    print(event)
    message_attributes = event["Records"][0]["messageAttributes"]
    job = message_attributes["Job"]["stringValue"]
    user_id = message_attributes["UserId"]["stringValue"]
    athlete_id = message_attributes["AthleteId"]["stringValue"]

    if job == "BACKFILL_ATHLETE":
        after = datetime.strptime("01/01/2020", "%m/%d/%Y")

        while True:
            before = after + timedelta(days=7)

            response = sqs.send_message(
                QueueUrl=STRAVA_API_QUEUE_URL,
                DelaySeconds=0,
                MessageAttributes={
                    "Job": {"DataType": "String", "StringValue": "FETCH_STRAVA_ACTIVITY"},
                    "AthleteId": {"DataType": "String", "StringValue": athlete_id},
                    "UserId": {"DataType": "String", "StringValue": user_id},
                    "BeforeDate": {"DataType": "String", "StringValue": before.strftime("%m/%d/%Y")},
                    "AfterDate": {"DataType": "String", "StringValue": after.strftime("%m/%d/%Y")}
                },
                MessageBody=(
                    "Get strava athlete activity for {user_id} for {before} to {after}".format(
                        user_id=user_id,
                        before=before.strftime("%m/%d/%Y"),
                        after=after.strftime("%m/%d/%Y")
                    )
                ),
                MessageGroupId="STRAVA-API"
            )
            print(response['MessageId'])
            after += timedelta(days=7)
            if after > datetime.now():
                break

    return {"status": "enqueued"}


# if __name__ == "__main__":
#     main("", "")
