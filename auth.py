import json
import urllib.request
from jose import jwk, jwt
from jose.utils import base64url_decode
import time
import os

region = "us-east-1"
userpool_id = os.environ["USER_POOL_ID"]
app_client_id = os.environ["USER_POOL_CLIENT_ID"]

keys_url = "https://cognito-idp.{region}.amazonaws.com/{userpool_id}/.well-known/jwks.json".format(
    region=region, userpool_id=userpool_id
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
            {
                "Action": "execute-api:Invoke",
                "Effect": effect,
                "Resource": "*"
            }
        ],
    }
    response = {"policyDocument": policy, "principalId": principal_id}
    # if context is not None:
    #     response["context"] = context

    return response


def main(event, context):
    if "authorizationToken" not in event:
        return generate_policy("DENY", "deny", event["methodArn"])
    token = event["authorizationToken"][7:]
    # get the kid from the headers prior to verification
    headers = jwt.get_unverified_headers(token)
    kid = headers["kid"]
    # search for the kid in the downloaded public keys
    key_index = -1
    for i in range(len(keys)):
        if kid == keys[i]["kid"]:
            key_index = i
            break
    if key_index == -1:
        print("Public key not found in jwks.json")
        return False
    # construct the public key
    public_key = jwk.construct(keys[key_index])
    # get the last two sections of the token,
    # message and signature (encoded in base64)
    message, encoded_signature = str(token).rsplit(".", 1)
    # decode the signature
    decoded_signature = base64url_decode(encoded_signature.encode("utf-8"))
    # verify the signature
    if not public_key.verify(message.encode("utf8"), decoded_signature):
        print("Signature verification failed")
        # return False
        return generate_policy("ENY", "Deny", event["methodArn"])
    print("Signature successfully verified")
    # since we passed the verification, we can now safely
    # use the unverified claims
    claims = jwt.get_unverified_claims(token)
    # additionally we can verify the token expiration
    if time.time() > claims["exp"]:
        print("Token is expired")
        return generate_policy(claims["sub"], "Deny", event["methodArn"])
        # return {"statusCode": 401, "body": '{"token_expired": "true"'}

    # and the Audience  (use claims['client_id'] if verifying an access token)
    if claims["aud"] != app_client_id:
        print("Token was not issued for this audience")
        return generate_policy(claims["sub"], "Deny", event["methodArn"])
    # now we can use the claims
    return generate_policy(claims["sub"], "Allow", event["methodArn"])


if __name__ == "__main__":
    main("", "")
