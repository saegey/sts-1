import json
import time
import urllib.request

import boto3
from jose import jwk, jwt
from jose.utils import base64url_decode

from lib.config import Config

config = Config()

dynamodb = boto3.resource("dynamodb", region_name=config.aws_region)
strava_auth_table = dynamodb.Table(config.strava_auth_table)
keys_url = (
    "https://cognito-idp.{region}.amazonaws.com/{userpool_id}"
    "/.well-known/jwks.json".format(
        region=config.aws_region, userpool_id=config.cognito_user_pool_id
    )
)
# # instead of re-downloading the public keys every time
# # we download them only on cold start
# # https://aws.amazon.com/blogs/compute/container-reuse-in-lambda/
with urllib.request.urlopen(keys_url) as f:
    response = f.read()
keys = json.loads(response.decode("utf-8"))["keys"]


# https://hasura.io/blog/best-practices-of-using-jwt-with-graphql/#jwt_structure
def generate_policy(principal_id, effect, resource, context=None):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Action": "execute-api:Invoke", "Effect": effect, "Resource": "*"}
        ],
    }
    response = {
        "policyDocument": policy,
        "principalId": principal_id,
        "context": {"athleteId": None},
    }
    dynamo_res = strava_auth_table.get_item(Key={"user_id": principal_id})
    if "Item" in dynamo_res:
        response["context"]["athleteId"] = dynamo_res["Item"]["athlete_id"]

    return response


def main(event, context):
    if "authorizationToken" not in event:
        return generate_policy("DENY", "deny", event["methodArn"])
    token = event["authorizationToken"][7:]
    headers = jwt.get_unverified_headers(token)
    kid = headers["kid"]
    key_index = -1
    for i in range(len(keys)):
        if kid == keys[i]["kid"]:
            key_index = i
            break
    if key_index == -1:
        print("Public key not found in jwks.json")
        return False

    public_key = jwk.construct(keys[key_index])
    message, encoded_signature = str(token).rsplit(".", 1)
    decoded_signature = base64url_decode(encoded_signature.encode("utf-8"))

    if not public_key.verify(message.encode("utf8"), decoded_signature):
        print("Signature verification failed")
        return generate_policy("ENY", "Deny", event["methodArn"])

    print("Signature successfully verified")
    claims = jwt.get_unverified_claims(token)

    if time.time() > claims["exp"]:
        print("Token is expired")
        return generate_policy(claims["sub"], "Deny", event["methodArn"])
        # return {"statusCode": 401, "body": '{"token_expired": "true"'}

    if claims["aud"] != config.cognito_user_pool_client_id:
        print("Token was not issued for this audience")
        return generate_policy(claims["sub"], "Deny", event["methodArn"])

    return generate_policy(claims["sub"], "Allow", event["methodArn"])


if __name__ == "__main__":
    main("", "")
