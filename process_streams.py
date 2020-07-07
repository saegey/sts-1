from lib.recent_athlete_peak import RecentAthletePeak
import boto3
import json
from config import Config
from pprint import pprint
from lib.activity_peak import ActivityPeak

config = Config()
PEAK_DURATIONS = [5, 60, 300, 600, 1200, 3600, 5400]
TYPE = {"rowing": ["Rowing"], "cycling": ["VirtualRide", "Ride"]}
dynamo_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")


def fill_values(time_stream, data_stream):
    new_data_stream = []
    if len(time_stream) != len(data_stream):
        return None
    if len(time_stream) == len(data_stream):
        return data_stream
    for i in range(0, len(time_stream) - 1):
        if i == len(time_stream):
            continue
        new_data_stream.append(data_stream[i])
        time_diff = time_stream[i + 1] - time_stream[i]
        if time_diff > 1:
            data_diff = data_stream[i + 1] - data_stream[i]
            for j in range(1, time_diff):
                new_data_stream.append(
                    data_stream[i] + ((data_diff / time_diff) * j))
    return new_data_stream


def calc_peak(num_seconds, data_stream, activity_id):
    if len(data_stream) == 0:
        return 0
    if num_seconds > len(data_stream):
        return None
    sums = []
    for w in range(len(data_stream)):
        if w + num_seconds > len(data_stream):
            break

        try:
            sums.append(sum(data_stream[w: w + num_seconds]))
        except:
            print("unable to sum datastream for {}".format(activity_id))
            return 0
    peak_total = sorted(sums)[-1]
    return peak_total / num_seconds


def main(event, context):
    for record in event["Records"]:
        # print(record)
        s3_events = json.loads(record['body'])
        if "Records" not in s3_events:
            print('test event', record['body'])
            continue

        for s3event in s3_events['Records']:
            s3_bucket = s3event['s3']['bucket']['name']
            filename = s3event['s3']['object']['key']

            activity_filename = "activity_{}".format(
                "_".join(filename.split("_")[1:]))
            response = s3_client.get_object(Bucket=s3_bucket, Key=filename)
            activity_raw_response = s3_client.get_object(
                Bucket=s3_bucket, Key=activity_filename
            )
            activity_res_body = json.load(activity_raw_response["Body"])
            res_body = json.load(response["Body"])

            athlete_id = int(filename.split("_")[1])
            activity_id = int(filename.split("_")[2].split(".")[0])
            print('load json file for {athlete_id} and activity: {activity_id}'.format(
                athlete_id=athlete_id, activity_id=activity_id))
            if "time" not in res_body:
                print('time metric not found')
                continue
            time_stream = res_body["time"]

            peaks_to_push = []
            for statistic in ["heartrate", "watts", "velocity_smooth"]:
                for duration in PEAK_DURATIONS:
                    if statistic not in res_body:
                        continue
                    data_stream = res_body[statistic]
                    normalized_stream = fill_values(time_stream, data_stream)
                    peak_value = calc_peak(
                        duration, normalized_stream, activity_id)
                    if peak_value is None:
                        continue
                    elapsed_time = activity_res_body["elapsed_time"] if activity_res_body["elapsed_time"] is not None else ""

                    item = {
                        "peak_id": "{activity_id}_{statistic}_{duration}".format(
                            activity_id=activity_id, statistic=statistic, duration=duration),
                        "peak_type": "{type}_{statistic}_{duration}".format(
                            type=activity_res_body["type"], statistic=statistic, duration=str(duration)),
                        "attribute": statistic,
                        "value": str(peak_value),
                        "activity_id": str(activity_id),
                        "athlete_id": str(athlete_id),
                        "duration": str(duration),
                        "start_date_local": activity_res_body["start_date_local"],
                        "name": activity_res_body["name"],
                        "type": activity_res_body["type"],
                        "trainer": str(activity_res_body["trainer"]),
                        "elapsed_time": elapsed_time,
                    }

                    if activity_res_body["distance"] is not None:
                        item["distance"] = str(activity_res_body["distance"])
                    if activity_res_body["suffer_score"] is not None:
                        item["suffer_score"] = str(activity_res_body["suffer_score"])
                    peaks_to_push.append(item)
            # pprint(peaks_to_push)
            activity_peak = ActivityPeak(peaks_to_push).save()
            RecentAthletePeak.enqueue(athlete_id)
    return True
