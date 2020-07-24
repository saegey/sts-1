import json
from jinja2 import Environment, FileSystemLoader
import os
import boto3
from boto3.dynamodb.conditions import Key
from stravalib.client import Client as StravaClient
from pprint import pprint
from config import Config
from lib.recent_athlete_peak import RecentAthletePeak
from lib.strava_activity import StravaActivity
from lib.activity_peak import ActivityPeak

config = Config()
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
strava_auth_table = dynamodb.Table(config.strava_auth_table)
sqs = boto3.client("sqs")


env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "templates"), encoding="utf8"
    )
)

HTML_HEADERS = {
    # Required for CORS support to work
    "Access-Control-Allow-Origin": "*",
    # Required for cookies, authorization headers with HTTPS
    "Access-Control-Allow-Credentials": True,
    "Content-Type": "text/html",
}

params = {
    "strava_client_id": config.strava_client_id,
    "user_pool": config.cognito_user_pool,
    "callback_url": config.callback_url,
    "user_pool_client_id": config.cognito_user_pool_client_id,
    "cognito_login_url": config.cognito_login_url,
    "stage": config.stage,
    "cognito_url": config.cognito_url
}


def root_view():
    template = env.get_template("main.html")

    return template.render(title=u"STS-1", params=params)


def strava_auth_view(authorize_url):
    template = env.get_template("strava_auth.html")
    return template.render(title=u"STS-1 Strava Auth", authorize_url=authorize_url, params=params)


def strava_callback(event, context):
    template = env.get_template("strava_callback.html")

    return {
        "statusCode": 200,
        "headers": HTML_HEADERS,
        "body": template.render(title=u"STS-1 Strava Callback", params=params),
    }


def logout(event, context):
    template = env.get_template("logout.html")
    return {
        "statusCode": 200,
        "headers": HTML_HEADERS,
        "body": template.render(title=u"STS-1 Logout", params=params),
    }


def backfill_athlete(event, context):
    user_id = event["requestContext"]["authorizer"]["principalId"]
    auth_response = strava_auth_table.get_item(Key={"user_id": user_id})
    # TODO - if no auth response it should return error

    response = sqs.send_message(
        QueueUrl=config.backfill_athlete_queue,
        DelaySeconds=0,
        MessageAttributes={
            "Job": {"DataType": "String", "StringValue": "BACKFILL_ATHLETE"},
            "UserId": {"DataType": "String", "StringValue": user_id},
            "AthleteId": {"DataType": "String", "StringValue": auth_response["Item"]["athlete_id"]},
        },
        MessageBody=(
            "Backfill athlete for user {user_id}".format(user_id=user_id)),
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
    strava_client.refresh_token = refresh_token
    strava_client.token_expires_at = expires_at

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
    print('saved strava auth credentials', response)
    # TODO - if no auth response it should return error

    response = sqs.send_message(
        QueueUrl=config.backfill_athlete_queue,
        DelaySeconds=0,
        MessageAttributes={
            "Job": {"DataType": "String", "StringValue": "BACKFILL_ATHLETE"},
            "UserId": {"DataType": "String", "StringValue": user_id},
            "AthleteId": {"DataType": "String", "StringValue": str(athlete.id)},
        },
        MessageBody=(
            "Backfill athlete for user {user_id}".format(user_id=user_id)),
    )
    print('enqueued strava backfill post authorization', response)
    return {
        "statusCode": 200,
        "body": '{"strava_authorized": true}',
    }


def strava_auth(event, context):
    strava_client = StravaClient()
    redirect_uri = "{callback_url}strava-callback".format(
        callback_url=config.callback_url
    )
    authorize_url = strava_client.authorization_url(
        client_id=config.strava_client_id, redirect_uri=redirect_uri,
    )
    return {
        "statusCode": 200,
        "headers": HTML_HEADERS,
        "body": strava_auth_view(authorize_url=authorize_url),
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
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
        "body": json.dumps(event["requestContext"]["authorizer"]),
    }


def activity_peaks(event, context):
    athlete_id = event["requestContext"]["authorizer"]["athleteId"]
    activity_id = event['pathParameters']['activityId']
    res = ActivityPeak.get_athlete_from_s3(athlete_id)

    peaks = []
    for peak_type in res.keys():
        index = 1
        for peak in res[peak_type]:
            if peak['activity_id'] == activity_id:
                peak['rank'] = index
                peaks.append(peak)
            index += 1

    return {
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
        "statusCode": 200,
        "body": json.dumps(peaks, default=str),
    }


def activity(event, context):
    athlete_id = event["requestContext"]["authorizer"]["athleteId"]
    activity_id = event['pathParameters']['activityId']
    s3_activity = StravaActivity.getFromS3(athlete_id, activity_id)

    return {
        "statusCode": 200,
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
        "body": json.dumps(s3_activity, default=str),
    }


def recent_peaks(event, context):
    user_id = event["requestContext"]["authorizer"]["principalId"]
    response = strava_auth_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    # print(response)
    athlete_id = response["Items"][0]["athlete_id"]
    res = RecentAthletePeak.fetch(athlete_id)
    formatted_items = []
    for item in res['Items']:
        formatted_items += item["data"]

    formatted_items.sort(
        key=lambda x: int(x['date_timestamp']), reverse=True)

    return {
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
        "statusCode": 200,
        "body": json.dumps(formatted_items, default=str),
    }


def main(event, context):
    return {"statusCode": 200, "headers": HTML_HEADERS, "body": root_view()}
