import boto3
from config import Config
from boto3.dynamodb.conditions import Key

config = Config()
dynamodb = boto3.resource("dynamodb", config.aws_region)
peaks_table = dynamodb.Table(config.athlete_peaks_table)


class ActivityPeak():
    @classmethod
    def getAll(cls, athlete_id, exclusive_start_key=None):
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
    def getTop(cls, athlete_id):
        items = []
        exclusive_start_key = None
        while True:
            print('querying results from dynamo', exclusive_start_key)
            results = cls.getAll(athlete_id, exclusive_start_key)
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
