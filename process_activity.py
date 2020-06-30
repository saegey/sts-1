import boto3
import os
import json


S3_BUCKET = os.getenv("BUCKET").split(".")[0]
ACTIVITIES_TABLE = os.environ["ACTIVITIES_TABLE"]

dynamo_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")


def main(event, context):
    # print(event, context)
    for record in event["Records"]:
        filename = record["s3"]["object"]["key"]
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=filename)
        res_body = json.load(response["Body"])
        athlete_id = int(filename.split("_")[1])
        activity_id = int(filename.split("_")[2].split(".")[0])

        resp = dynamo_client.put_item(
            TableName=ACTIVITIES_TABLE,
            Item={
                "athlete_id": {"S": str(athlete_id)},
                "activity_id": {"S": str(activity_id)},
                "start_date_local": {"S": res_body["start_date_local"]},
                "name": {"S": res_body["name"]},
                "distance": {"N": str(res_body["distance"])},
                "type": {"S": res_body["type"]},
                "trainer": {"S": str(res_body["trainer"])},
                "elapsed_time": {"S": res_body["elapsed_time"]},
                "suffer_score": {"N": str(res_body["suffer_score"])},
            },
        )
        print(resp)

        # print(
        #     {
        #         "athlete_id": athlete_id,
        #         "activity_id": activity_id,
        #         "start_date_local": res_body["start_date_local"],
        #         "name": res_body["name"],
        #         "distance": res_body["distance"],
        #         "type": res_body["type"],
        #         "trainer": res_body["trainer"],
        #         "elapsed_time": res_body["elapsed_time"],
        #         "suffer_score": res_body["suffer_score"],
        #     }
        # )
        # athlete id, name, start_date_local, distance,
        # id, type, trainer, elapsed_time, suffer_score
