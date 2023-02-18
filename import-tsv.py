from google.cloud import storage
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from google.cloud.sql.connector import Connector, IPTypes
import google.cloud.logging
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os, sqlalchemy, pg8000

# REQUIREMENTS
#
# pandas
# gcsfs
# fsspec
# google.cloud
# "cloud-sql-python-connector[pg8000]"
# pg8000


# set up some global variables:
load_dotenv(find_dotenv()) # load environment variables
bucket_name = os.environ.get("gcs_bucket_name")
project_id = os.environ.get("GCP_PROJECT")
log_name = os.environ.get("LOG_ID")

# Set up Google cloud logging:
log_client = google.cloud.logging.Client(project=project_id)
logger = log_client.logger(name=log_name)



# [START cloud_sql_postgres_sqlalchemy_connect_connector]
# From https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/cloud-sql/postgres/sqlalchemy/connect_connector.py
# thx Google ;)





# connect_with_connector initializes a connection pool for a
# Cloud SQL instance of Postgres using the Cloud SQL Python Connector.
def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.

    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]  # e.g. 'project:region:instance'
    db_user = os.environ["DB_USER"]  # e.g. 'my-db-user'
    db_pass = os.environ["DB_PASS"]  # e.g. 'my-db-password'
    db_name = os.environ["DB_NAME"]  # e.g. 'my-database'

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # [START_EXCLUDE]
        # Pool size is the maximum number of permanent connections to keep.
        pool_size=5,

        # Temporarily exceeds the set pool_size if no connections are available.
        max_overflow=2,

        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.

        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        pool_timeout=30,  # 30 seconds

        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # re-established
        pool_recycle=1800,  # 30 minutes
        # [END_EXCLUDE]
    )
    return pool

# [END cloud_sql_postgres_sqlalchemy_connect_connector]


def retrieve_tsv(object):
    path = 'gs://' + bucket_name + '/' + object
    print(f'Loading {path} into a pandas DataFrame...')
    # gs://birdwatch-scraper_public-data/2022/11/12/ratings.tsv
    df = pd.read_csv(path, sep='\t', header=0)
    return df


def main(event_data, context):
    # We have to include event_data and context because these will be passed as arguments when invoked as a Cloud Function
    # and the runtime will freak out if the function only accepts 0 arguments... go figure
    print('Started Execution')
    
    
    # Set up a db connection pool
    db = connect_with_connector()
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        conn = db.raw_connection()
        cur = conn.cursor()
        print('db connection seems to have worked')
        logger.log('Database Connection was successful')
    except Exception as e:
        print('db connection failure')
        print(e)
        logger.log_struct(
            {
                "message": "Database Connection Failure",
                "severity": "ERROR",
                "exception": e
            })
        quit()

    

    # Get the most recent downloaded file
    #   (with error handling for if a file is missing for whatever reason)
    file_path = date.today().strftime("%Y/%m/%d")

    ## Get notes ##
    try:
        logger.log('Retrieving notes.tsv', severity="INFO")
        object = file_path + '/notes.tsv'

        df = retrieve_tsv(object)
        print(df.info())
        print(df)

        # # Insert data from that file into the db:
        print('Now converting dataframe into sql and placing into a temporary table')
        df.to_sql('temp_notes', db, if_exists='replace')
        logger.log('Copying temp_notes into the notes table', severity="INFO")
        print('Now copying into the real table...')
        with db.begin() as cn:
            sql = """INSERT INTO notes
                    SELECT *
                    FROM temp_notes
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()
    except Exception as e:
        print('Error when getting notes:')
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving notes.tsv",
                "severity": "WARNING",
                "exception": e
            })

    print('Done! Now refreshing the db connection...')
    try:
        conn.close()
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        conn = db.raw_connection()
        cur = conn.cursor()
        print('db connection seems to have worked')
    except:
        print('db connection failure')
        quit()
    

    # print('Now inserting data from table_temp into notes - and skipping duplicates')
    # cur.execute("""Insert into notes select * From table_temp ON CONFLICT DO NOTHING;""");

    # print('Now dropping the temporary table')
    # cur.execute("""DROP TABLE table_temp CASCADE;""");  # You can drop if you want to but the replace option in to_sql will drop and recreate the table
    # conn.commit()

    ## Get ratings ##
    try:
        logger.log('Retrieving ratings.tsv', severity="INFO")
        object = file_path + '/ratings.tsv'
        df = retrieve_tsv(object)
        df['ratingsId'] = df[['noteId', 'raterParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        print('Now converting dataframe into sql and placing into a temporary table')
        df.to_sql('temp_ratings', db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Copying temp_ratings into ratings', severity="INFO")
        with db.begin() as cn:
            sql = """INSERT INTO ratings
                    SELECT *
                    FROM temp_ratings
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()
    except Exception as e:
        print('Error when getting ratings:')
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving ratings.tsv",
                "severity": "WARNING",
                "exception": e
            })

    print('Done! Now refreshing the db connection...')
    try:
        conn.close()
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        conn = db.raw_connection()
        cur = conn.cursor()
        print('db connection seems to have worked')
    except:
        print('db connection failure')
        quit()

    ## Get noteStatusHistory ##
    try:
        logger.log('Retrieving noteStatusHistory.tsv', severity="INFO")
        object = file_path + '/noteStatusHistory.tsv'
        df = retrieve_tsv(object)
        df['statusId'] = df[['noteId', 'noteAuthorParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        print('Now converting dataframe into sql and placing in a temporary table')
        df.to_sql('temp_status', db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Retrieving temp_status into status_history', severity="INFO")
        with db.begin() as cn:
            sql = """INSERT INTO status_history
                    SELECT *
                    FROM temp_status
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()
    except Exception as e:
        print('Error when getting noteStatusHistoyr:')
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving noteStatusHistory.tsv",
                "severity": "WARNING",
                "exception": e
            })

    print('Done! Now refreshing the db connection...')
    try:
        conn.close()
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        conn = db.raw_connection()
        cur = conn.cursor()
        print('db connection seems to have worked')
    except:
        print('db connection failure')
        quit()

    ## Get userEnrollmentStatus ##
    try:
        logger.log('Retrieving userEnrollmentStatus.tsv', severity="INFO")
        object = file_path + '/userEnrollmentStatus.tsv'
        df = retrieve_tsv(object)
        # Participant Ids may be duplicated (because the same user's status may change), so we concatenate with the timestamp to create a primary key
        df['statusId'] = df[['participantId', 'timestampOfLastStateChange']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        print('Now converting dataframe into sql and placing in a temporary table')
        df.to_sql('temp_userenrollment', db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Copying temp_userenrollment into enrollment_status', severity="INFO")
        with db.begin() as cn:
            sql = """INSERT INTO enrollment_status
                    SELECT *
                    FROM temp_userenrollment
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()
    except Exception as e:
        print('Error when getting userEnrollmentStatus:')
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving userEnrollmentStatus.tsv",
                "severity": "WARNING",
                "exception": e
            })

    # Clean up temp tables
    print('Now deleting temporary tables!')
    try:
        cur.execute("""DROP TABLE temp_notes CASCADE;""");
        cur.execute("""DROP TABLE temp_ratings CASCADE;""");
        cur.execute("""DROP TABLE temp_status CASCADE;""");
        cur.execute("""DROP TABLE temp_userenrollment CASCADE;""");
        logger.log("Tempotary tables dropped", severity="INFO")
    except Exception as e:
        print('Unable to drop a temp table. Does it actually exist?')
        print(e)
        logger.log_struct(
            {
                "message": "Error when dropping the the temporary tables",
                "severity": "WARNING",
                "exception": e
            })
    conn.commit()

    # close the db connection
    conn.close()



    print('Done!')

if __name__ == "__main__":
    start_time = datetime.now()
    print('FYI: Script started directly as __main__')
    logger.log('Script Execution Started', severity="INFO")
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...
    end_time = datetime.now()
    total_time = end_time - start_time
    print(f'Total execution was: {total_time}')
    logger.log('Script execution finished', severity="INFO")