import boto3
from stravalib.client import Client as StravaClient
from datetime import datetime, timedelta
import json
from ratelimiter import RateLimiter
from config import Config
from lib.strava_activity import StravaActivity
from lib.strava_athlete import StravaAthlete
from lib.strava_enqueue import EnqStravaApiActivities
from lib.activity_peak import ActivityPeak
from lib.recent_athlete_peak import RecentAthletePeak
from pprint import pprint

STREAM_TYPES = [
    "time",
    "distance",
    "latlng",
    "altitude",
    "grade_smooth",
    "velocity_smooth",
    "heartrate",
    "watts",
]
strava_client = StravaClient()
config = Config()

s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
strava_auth_table = dynamodb.Table(config.strava_auth_table)


def get_and_save_strava_streams(strava_client, activity_id, athlete_id):
    streams_filename = "streams_{athlete_id}_{activity_id}.json".format(
        athlete_id=athlete_id, activity_id=activity_id)
    streams = strava_client.get_activity_streams(
        activity_id, types=STREAM_TYPES
    )
    formatted_streams = {}
    if streams is None:
        return False

    for stream in streams.keys():
        formatted_streams[stream] = streams[stream].data

    s3_client.put_object(
        Body=json.dumps(formatted_streams, default=str),
        Bucket=config.strava_api_s3_bucket,
        Key=streams_filename,
    )
    print('saving streams to {bucket}:{key}'.format(
        bucket=config.strava_api_s3_bucket, key=streams_filename))
    return True


@RateLimiter(max_calls=600, period=900)
def strava_api_call(job_type, message_attribs, athlete_id, user_id):
    if job_type == "FETCH_STRAVA_ACTIVITY":
        athlete = StravaAthlete(user_id=user_id)
        stravaActivities = StravaActivity.fetch(
            athlete=athlete,
            before=datetime.strptime(
                message_attribs['BeforeDate']['stringValue'], "%m/%d/%Y"),
            after=datetime.strptime(
                message_attribs['AfterDate']['stringValue'], "%m/%d/%Y"))

        for stravaActivity in stravaActivities:
            stravaActivity.saveToS3()
            stravaActivity.enqueueStreamFetch()

    elif job_type == "FETCH_STRAVA_STREAM":
        activity_id = message_attribs['ActivityId']['stringValue']
        get_and_save_strava_streams(
            strava_client=strava_client, athlete_id=athlete_id, activity_id=activity_id)


def enqueue_strava_athlete_sync(event, context):
    athletes = StravaAthlete.get_all()
    for athlete in athletes:
        stravaActivities = StravaActivity.fetch(athlete=athlete)

        for stravaActivity in stravaActivities:
            print('enqueue stream fetch and save activity to s3: ', stravaActivity)
            stravaActivity.saveToS3()
            stravaActivity.enqueueStreamFetch()

        athlete.last_sync_at = datetime.now().timestamp()
        athlete.save()


def calculate_peaks_for_athlete(event, context):
    for athlete in event["Records"]:
        athlete_id = athlete["messageAttributes"]["AthleteId"]["stringValue"]

        print(athlete_id)
        results = ActivityPeak.get_top(athlete_id)
        # pprint(results)
        peaks_filename = "peaks_{athlete_id}.json".format(
            athlete_id=athlete_id)

        s3_client.put_object(
            Body=json.dumps(results, default=str),
            Bucket=config.strava_api_s3_bucket,
            Key=peaks_filename,
        )
        print('writing peaks file to => {filename}'.format(
            filename=peaks_filename))


def process_peaks(event, context):
    # print(event)
    for record in event["Records"]:
        body = json.loads(record['body'])
        for s3_file in body['Records']:
            bucket = s3_file['s3']['bucket']['name']
            key = s3_file['s3']['object']['key']
            response = s3_client.get_object(Bucket=bucket, Key=key)
            res_body = json.load(response["Body"])

            recent_peaks = {}
            for peak_type in res_body:
                for i, peak in enumerate(res_body[peak_type][0:9], 1):
                    peak_date = datetime.strptime(
                        peak["start_date_local"], "%Y-%m-%dT%H:%M:%S")
                    if peak_date > (datetime.now() - timedelta(days=30)):
                        peak["date_timestamp"] = int(peak_date.timestamp())
                        peak["rank"] = i
                        if peak_type not in recent_peaks:
                            recent_peaks[peak_type] = []
                        recent_peaks[peak_type].append(peak)

            RecentAthletePeak.bulk_save(recent_peaks)


def fetch_strava_api(event, context):
    for record in event['Records']:
        print(record)
        message_attribs = record['messageAttributes']
        job_type = message_attribs['Job']['stringValue']
        athlete_id = message_attribs['AthleteId']['stringValue']
        user_id = message_attribs['UserId']['stringValue']

        strava_client.access_token = StravaAthlete(user_id).access_token
        strava_api_call(job_type=job_type, message_attribs=message_attribs,
                        athlete_id=athlete_id, user_id=user_id)


def enqueue_strava_backfill(event, context):
    print(event)
    message_attributes = event["Records"][0]["messageAttributes"]
    job = message_attributes["Job"]["stringValue"]
    user_id = message_attributes["UserId"]["stringValue"]
    athlete_id = message_attributes["AthleteId"]["stringValue"]

    if job == "BACKFILL_ATHLETE":
        enqueue_activity = EnqStravaApiActivities(user_id, athlete_id)
        after = datetime.strptime("01/01/2015", "%m/%d/%Y")

        while True:
            before = after + timedelta(days=7)
            response = enqueue_activity.queue(before, after)
            print(response)
            after += timedelta(days=7)

            if after > datetime.now():
                break

    return {"status": "enqueued"}


# if __name__ == "__main__":
#     main("", "")
