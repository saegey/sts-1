import json
from jinja2 import Environment, FileSystemLoader
from os import path
import boto3
import os
from config import Config

env = Environment(
    loader=FileSystemLoader(
        path.join(path.dirname(__file__), "templates"), encoding="utf8"
    )
)

config = Config()

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
        "strava_client_id": config.strava_client_id,
        "user_pool": config.cognito_user_pool,
        "callback_url": config.callback_url,
        "cognito_url": config.cognito_url,
        "user_pool_client_id": config.cognito_user_pool_client_id,
        "cognito_login_url": config.cognito_login_url,
        "stage": config.stage
    }
    return {"statusCode": 200, "headers": headers, "body": root_view(params)}
