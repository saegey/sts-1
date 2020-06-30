import json
from jinja2 import Environment, FileSystemLoader
from os import path
import boto3
import os

env = Environment(
    loader=FileSystemLoader(
        path.join(path.dirname(__file__), "templates"), encoding="utf8"
    )
)

ssm_client = boto3.client("ssm", region_name="us-east-1")


def get_secret(key):
    resp = ssm_client.get_parameter(Name=key, WithDecryption=True)
    return resp["Parameter"]["Value"]


def root_view(params):
    template = env.get_template("graph.html",)
    return template.render(
        title=u"STS-1 Main",
        params=params
    )


def main(event, context):
    headers = {
        # Required for CORS support to work
        "Access-Control-Allow-Origin": "*",
        # Required for cookies, authorization headers with HTTPS
        "Access-Control-Allow-Credentials": True,
        "Content-Type": "text/html",
    }
    params = {
        "strava_client_id": get_secret("STRAVA_CLIENT_ID"),
        "user_pool": os.environ["USER_POOL"],
        "callback_url": os.environ["URL"],
        "cognito_url": os.environ["COGNITO_URL"],
        "user_pool_client_id": os.environ["USER_POOL_CLIENT_ID"],
        "cognito_login_url": os.environ["COGNITO_LOGIN_URL"],
        "stage": os.environ["STAGE"]
    }
    return {"statusCode": 200, "headers": headers, "body": root_view(params)}
