import boto3
from moto import mock_dynamodb2
from lib.config import Config

config = Config()


@mock_dynamodb2
def strava_auth_table():
    dynamodb = boto3.resource("dynamodb", config.aws_region)
    return dynamodb.create_table(
        TableName=config.strava_auth_table,
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"}
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    )


@mock_dynamodb2
def peaks_table() -> None:
    dynamodb = boto3.resource("dynamodb", config.aws_region)
    return dynamodb.create_table(
        TableName=config.athlete_peaks_table,
        KeySchema=[
            {"AttributeName": "athlete_id", "KeyType": "HASH"},
            {"AttributeName": "peak_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "athlete_id", "AttributeType": "S"},
            {"AttributeName": "peak_id", "AttributeType": "S"},
            {"AttributeName": "peak_type", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 2,
            "WriteCapacityUnits": 2,
        },
        GlobalSecondaryIndexes=[
            {
                "IndexName": "peaks_type",
                "KeySchema": [
                    {"AttributeName": "athlete_id", "KeyType": "HASH"},
                    {"AttributeName": "peak_type", "KeyType": "RANGE"},
                ],
                "Projection": {
                    "NonKeyAttributes": [
                        "start_date_local",
                        "name",
                        "activity_id",
                        "value",
                    ],
                    "ProjectionType": "INCLUDE",
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 2,
                },
            }
        ],
    )
