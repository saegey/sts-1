from pprint import pprint
import boto3
from config import Config
from boto3.dynamodb.conditions import Key
from pprint import pprint
from config import Config

config = Config()
dynamodb = boto3.resource("dynamodb", config.aws_region)
recent_peaks_table = dynamodb.Table(config.recent_athlete_peaks_table)
sqs = boto3.client("sqs")
config = Config()


class RecentAthletePeak():
    @classmethod
    def enqueue(cls, athlete_id):
        sqs.send_message(
            QueueUrl=config.recent_athlete_peaks_to_s3,
            MessageAttributes={
                "AthleteId": {"DataType": "String", "StringValue": str(athlete_id)}
            },
            MessageBody=(
                "recent-athlete-peak-{athlete_id}".format(athlete_id=athlete_id)),
            MessageGroupId="RECENT-ATHLETE-PEAKS"
        )
        print('queuing recent peaks for {athlete_id}'.format(
            athlete_id=athlete_id))

    @classmethod
    def divide_chunks(cls, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @classmethod
    def bulk_save(cls, data):

        peak_chunks = list(cls.divide_chunks(list(data.keys()), 20))
        for chunk in peak_chunks:
            print('chunk:', chunk)
            with recent_peaks_table.batch_writer() as batch:
                for key in chunk:
                    batch.put_item(Item={
                        'athlete_id': data[key][0]['athlete_id'],
                        'peak_type': key,
                        'data': data[key]
                    })
                    print('put', key)

    @classmethod
    def fetch(cls, athlete_id):
        peaks_response = recent_peaks_table.query(
            KeyConditionExpression=Key('athlete_id').eq(
                athlete_id)
        )
        return peaks_response
