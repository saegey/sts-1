import json
from datetime import datetime, timedelta

import boto3
from stravalib.client import Client as StravaClient

from lib.config import Config

s3_client = boto3.client("s3")
sqs = boto3.client("sqs")
config = Config()


class StravaActivity:
    def __init__(self, data, user_id):
        self.data = data
        self.user_id = user_id

    @classmethod
    def getFromS3(cls, athelete_id, activity_id):
        activity_filename = "activity_{athlete_id}_{activity_id}.json".format(
            athlete_id=athelete_id, activity_id=activity_id
        )
        s3_response = s3_client.get_object(
            Key=activity_filename, Bucket=config.strava_api_s3_bucket
        )
        return json.load(s3_response["Body"])

    def saveToS3(self):
        activity_filename = "activity_{athlete_id}_{activity_id}.json".format(
            athlete_id=self.data.athlete.id, activity_id=self.data.id
        )
        s3_client.put_object(
            Body=json.dumps(self.data.to_dict(), sort_keys=True, default=str),
            Bucket=config.strava_api_s3_bucket,
            Key=activity_filename,
        )
        print(
            "saving activity to {bucket}:{key}".format(
                bucket=config.strava_api_s3_bucket, key=activity_filename
            )
        )

    def enqueueStreamFetch(self):
        response = sqs.send_message(
            QueueUrl=config.strava_api_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                "Job": {
                    "DataType": "String",
                    "StringValue": "FETCH_STRAVA_STREAM",
                },
                "AthleteId": {
                    "DataType": "String",
                    "StringValue": str(self.data.athlete.id),
                },
                "UserId": {
                    "DataType": "String",
                    "StringValue": str(self.user_id),
                },
                "ActivityId": {
                    "DataType": "String",
                    "StringValue": str(self.data.id),
                },
            },
            MessageBody=(
                "Get strava athlete stream for \
                    {athlete_id} for activity {activity_id}".format(
                    athlete_id=self.data.athlete.id, activity_id=self.data.id
                )
            ),
            MessageGroupId="STRAVA-API",
        )

        return response

    @classmethod
    def fetch(cls, athlete, before=None, after=None, limit=50):
        strava_client = StravaClient()
        strava_client.access_token = athlete.access_token

        before = datetime.now() if before is None else before
        after = (
            (datetime.fromtimestamp(athlete.last_sync_at) - timedelta(days=1))
            if after is None
            else after
        )
        print(before, after)
        activities_res = strava_client.get_activities(
            before=before, after=after, limit=50
        )

        activities = []
        for activity_res in activities_res:
            activities.append(
                StravaActivity(data=activity_res, user_id=athlete.user_id)
            )

        return activities
