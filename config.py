import os
import json
import boto3


class Config():
    def __init__(self):
        self.ssm_client = boto3.client("ssm", region_name="us-east-1")
        self.strava_auth_table = os.environ["STRAVA_AUTH_TABLE"]
        self.strava_api_queue_url = os.environ["STRAVA_API_QUEUE_URL"]
        self.backfill_athlete_queue = os.environ["SQS_QUEUE_URL"]
        self.athlete_peaks_table = os.environ["PEAKS_TABLE"]
        self.strava_api_uri = "https://www.strava.com/api/v3"
        self.strava_api_s3_bucket = os.getenv("BUCKET").split(".")[0]
        self.strava_client_id = self.get_secret("STRAVA_CLIENT_ID")
        self.strava_client_secret = self.get_secret("STRAVA_CLIENT_SECRET")
        self.cognito_user_pool = os.environ["USER_POOL"]
        self.cognito_user_pool_client_id = os.environ["USER_POOL_CLIENT_ID"]
        self.cognito_login_url = os.environ["COGNITO_LOGIN_URL"]
        self.cognito_user_pool_id = os.environ["USER_POOL_ID"]
        self.cognito_url = os.environ["COGNITO_URL"]
        self.callback_url = os.environ["URL"]
        self.stage = os.environ["STAGE"]
        self.aws_region = "us-east-1"

    def get_secret(self, key):
        resp = self.ssm_client.get_parameter(
            Name=key, WithDecryption=True)
        return resp["Parameter"]["Value"]

    def __str__(self):
        return json.dumps(self.__dict__, default=str)
