import boto3
from config import Config
from boto3.dynamodb.conditions import Key
from datetime import datetime
from decimal import Decimal
import json

config = Config()
dynamodb = boto3.resource("dynamodb", config.aws_region)
peaks_table = dynamodb.Table(config.athlete_peaks_table)
s3_client = boto3.client("s3")

class ActivityPeak():
    def __init__(self, dataset):
        self.dataset = dataset

    def save(self):
        with peaks_table.batch_writer() as batch:
            for row in self.dataset:
                print("batch saving activity => ",
                      row["activity_id"], " peak_id: ", row["peak_id"])
                batch.put_item({
                    "activity_id": row["activity_id"],
                    "athlete_id": row["athlete_id"],
                    "attribute": row["attribute"],
                    "distance": Decimal(row["distance"]),
                    "duration": int(row["duration"]),
                    "elapsed_time": row["elapsed_time"],
                    "name": row["name"],
                    "peak_id": row["peak_id"],
                    "peak_type": row["peak_type"],
                    "start_date_local": row["start_date_local"],
                    "trainer": row["trainer"],
                    "type": row["type"],
                    "value": Decimal(row["value"]),
                    "last_updated": int(datetime.now().timestamp())
                })

    @classmethod
    def get_athlete_from_s3(cls, athlete_id):
        peaks_filename = "peaks_{athlete_id}.json".format(
            athlete_id=athlete_id,
        )
        s3_response = s3_client.get_object(
            Key=peaks_filename,
            Bucket=config.strava_api_s3_bucket)
        return json.load(s3_response["Body"])

    @classmethod
    def get_all(cls, athlete_id, exclusive_start_key=None):
        peaks_response = {}
        if exclusive_start_key:
            peaks_response = peaks_table.query(
                KeyConditionExpression=Key('athlete_id').eq(
                    athlete_id),
                Limit=1000,
                ExclusiveStartKey=exclusive_start_key
            )
        else:
            peaks_response = peaks_table.query(
                KeyConditionExpression=Key('athlete_id').eq(
                    athlete_id),
                Limit=1000,
            )
        return peaks_response

    @classmethod
    def get_top(cls, athlete_id):
        items = []
        exclusive_start_key = None
        while True:
            print('querying results from dynamo', exclusive_start_key)
            results = cls.get_all(athlete_id, exclusive_start_key)
            items = items + results['Items']
            if "LastEvaluatedKey" not in results:
                break
            else:
                exclusive_start_key = results['LastEvaluatedKey']

        # print(items)
        print(results['ScannedCount'], results['ResponseMetadata'])
        print('retrieved {num} rows from dynamo peaks table'.format(
            num=len(items)))
        peaks_organized = {}
        for item in items:
            key = item['peak_type'].lower()
            if key not in peaks_organized:
                peaks_organized[key] = []
            peaks_organized[key].append(item)

        for key in peaks_organized.keys():
            peaks_organized[key].sort(
                key=lambda x: x['value'], reverse=True)
            peaks_organized[key] = peaks_organized[key]

        return peaks_organized
