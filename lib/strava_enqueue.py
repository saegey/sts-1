import boto3
from config import Config

config = Config()
sqs = boto3.client("sqs")

class EnqStravaApiActivities():
    def __init__(self, user_id, athlete_id):
        self.user_id = user_id
        self.athlete_id = athlete_id

    def queue(self, before, after):
        response = sqs.send_message(
            QueueUrl=config.strava_api_queue_url,
            DelaySeconds=0,
            MessageAttributes={
                "Job": {"DataType": "String", "StringValue": "FETCH_STRAVA_ACTIVITY"},
                "AthleteId": {"DataType": "String", "StringValue": self.athlete_id},
                "UserId": {"DataType": "String", "StringValue": self.user_id},
                "BeforeDate": {"DataType": "String", "StringValue": before.strftime("%m/%d/%Y")},
                "AfterDate": {"DataType": "String", "StringValue": after.strftime("%m/%d/%Y")}
            },
            MessageBody=(
                "Get strava athlete activity for {user_id} for {before} to {after}".format(
                    user_id=self.user_id,
                    before=before.strftime("%m/%d/%Y"),
                    after=after.strftime("%m/%d/%Y")
                )
            ),
            MessageGroupId="STRAVA-API"
        )
        return response
