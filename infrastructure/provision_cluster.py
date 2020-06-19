import configparser
import boto3
import json

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

CLUSTER_IDENTIFIER = config.get("CLUSTER", "IDENTIFIER")
DBNAME = config.get("CLUSTER", "DB_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = config.get("CLUSTER", "DB_PORT")
IAM_ROLE_NAME = config.get("IAM_ROLE", "ROLE_NAME")


ec2 = boto3.resource(
    "ec2",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

s3 = boto3.resource(
    "s3",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

iam = boto3.client(
    "iam",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)


def create_redshift_cluster():
    try:
        print("Creating a new IAM Role")
        iam.create_role(
            Path="/",
            RoleName=IAM_ROLE_NAME,
            Description="Allows redshift to access s3.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except iam.exceptions.EntityAlreadyExistsException:
        print("IAM role already exist")

    # attaching s3 read-only policy to created role
    iam.attach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )

    # get role arn
    role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)["Role"]["Arn"]

    try:
        redshift.create_cluster(
            # hardware
            ClusterType="multi-node",
            NodeType="dc2.large",
            NumberOfNodes=4,
            # credentials
            DBName=DBNAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            # roles
            IamRoles=[role_arn],
        )
    except redshift.exceptions.ClusterAlreadyExistsFault:
        print("Cluster already already exist")


def add_security_group():
    cluster = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]

    try:
        vpc = ec2.Vpc(id=cluster["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]

        if defaultSg.group_name == "default":
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": int(DB_PORT),
                        "ToPort": int(DB_PORT),
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    create_redshift_cluster()
    add_security_group()
