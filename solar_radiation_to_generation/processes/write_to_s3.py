from io import StringIO
import boto3


def write_to_s3(df, bucket, team_id, email, query_id, resolution):
    """
    Write the dataframe to S3 under the path bucket/TID/email/query_id.csv

    :param df: the dataframe that is about to be written
    :type df: pd.DataFrame
    :param bucket: the Object's bucket identifier. This must be set.
    :type bucket: str
    :param team_id: current session team id
    :type team_id: str
    :param email: current user's email address
    :type email: str
    :param query_id: current query id
    :type query_id: str
    :param resolution: current resolution
    :type resolution: str
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    key = 'TID' + team_id + '/' + email + '/' + query_id + '.csv'
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
