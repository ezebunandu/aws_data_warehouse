import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

CLUSTER_IDENTIFIER = config.get("SETUP", "IDENTIFIER")
IAM_ROLE_NAME = config.get("IAM_ROLE", "ROLE_NAME")

iam = boto3.client(
    "iam", region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

if __name__ == "__main__":
    redshift.delete_cluster(
        ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )

    iam.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=IAM_ROLE_NAME)
