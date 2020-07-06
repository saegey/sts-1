import boto3
from config import Config
from datetime import datetime, timedelta
import requests
import json

config = Config()
dynamodb = boto3.resource("dynamodb", config.aws_region)
strava_auth_table = dynamodb.Table(config.strava_auth_table)


class StravaAthlete():
    def __init__(self, user_id, refresh_token=None, access_token=None, athlete_id=None, expires_at=None, last_sync_at=None):
        self.user_id = user_id
        self.refresh_token = refresh_token
        self.access_token = access_token
        self.athlete_id = athlete_id
        self.expires_at = expires_at
        self.last_sync_at = last_sync_at
        if self.access_token is None or self.expires_at < datetime.now().timestamp():
            self.get_access_token()

    def save(self):
        response = strava_auth_table.put_item(
            Item={
                'user_id': self.user_id,
                'refresh_token': self.refresh_token,
                'access_token': self.access_token,
                'athlete_id': self.athlete_id,
                'expires_at': self.expires_at,
                'last_sync_at': int(self.last_sync_at)
            }
        )
        print(response)
        return True

    def __str__(self):
        return json.dumps(self.__dict__, default=str)

    def get_access_token(self):
        res = strava_auth_table.get_item(Key={"user_id": self.user_id})
        self.access_token = res["Item"]["access_token"]
        self.refresh_token = res["Item"]["refresh_token"]
        self.athlete_id = res["Item"]["athlete_id"]
        self.expires_at = res["Item"]["expires_at"]
        if "last_sync_at" in res["Item"]:
            self.last_sync_at = res["Item"]["last_sync_at"]

        if (datetime.now().timestamp() < self.expires_at):
            print('access token still valid', self.access_token)
            return self.access_token

        print(self.access_token)
        return self.fetch_new_token()

    def refresh_token_strava(self):
        raw_res = requests.post(
            "{url}/oauth/token".format(url=config.strava_api_uri),
            data={
                "client_id": config.strava_client_id,
                "client_secret": config.strava_client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
        )
        res = raw_res.json()
        return res

    def fetch_new_token(self):
        strava_res = self.refresh_token_strava()
        print("new token returned: {token}".format(token=str(strava_res)))
        print("updating token for {athlete_id}".format(
            athlete_id=self.athlete_id))

        token_update_res = strava_auth_table.put_item(
            Item={
                "user_id": self.user_id,
                "access_token": strava_res["access_token"],
                "refresh_token": strava_res["refresh_token"],
                "expires_at": strava_res["expires_at"],
                "athlete_id": str(self.athlete_id),
            }
        )
        print(token_update_res)
        self.access_token = strava_res["access_token"]
        self.refresh_token = strava_res["refresh_token"]
        self.expires_at = strava_res["expires_at"]

        return self.access_token

    @classmethod
    def get_all(cls):
        results = []
        response = strava_auth_table.scan()
        for strava_creds in response['Items']:
            if 'access_token' not in strava_creds or strava_creds['access_token'] is None:
                continue
            last_sync_at = strava_creds['last_sync_at'] if 'last_sync_at' in strava_creds else 1436029687

            athlete = StravaAthlete(
                access_token=strava_creds['access_token'],
                user_id=strava_creds['user_id'],
                refresh_token=strava_creds['refresh_token'],
                expires_at=strava_creds['expires_at'],
                athlete_id=strava_creds['athlete_id'],
                last_sync_at=last_sync_at,
            )
            results.append(athlete)
        return results
