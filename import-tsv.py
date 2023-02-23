from google.cloud import storage
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
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
    logger.log_struct(
            {
                "message": "Retrieving TSV and loading into Pandas dataframe",
                "severity": "INFO",
                "object": str(object),
                "gcs-path": str(path)
            })
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
        print(str(type(e)))
        logger.log_struct(
            {
                "message": "Database Connection Failure",
                "severity": "ERROR",
                "exception": str(type(e))
            })
        quit()

    

    # Get the most recent downloaded file
    #   (with error handling for if a file is missing for whatever reason)
    file_path = os.environ.get("DATE_OVERRIDE", date.today().strftime("%Y/%m/%d"))

    ## Get notes ##
    try:
        object = file_path + '/notes.tsv'
        table_name = 'temp_notes_' + date.today().strftime("%Y%m%d")
        df = retrieve_tsv(object)
        print(df.info())
        print(df)
        # Only keep the top 25% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.75)
        df.drop(df.tail(drop).index, inplace = True)
        logger.log_struct(
            {
                "message": 'Dropped rows from dataframe',
                "original-size": str(size),
                "dropped-rows": str(drop),
                "new-size": str(df.shape[0]),
                "severity": 'INFO',
            }
        )
        print("***")
        print(df)
        # # Insert data from that file into the db:
        print(f'Now converting dataframe into sql and placing into a temporary table called {table_name}')
        logger.log_struct(
            {
                "message": 'Now converting dataframe into sql and placing into a temporary table',
                "severity": "INFO",
                "object": str(object),
                "table-name": table_name
            }
        )

        df.to_sql(table_name, db, if_exists='replace')
        logger.log('Copying temp_notes into the notes table', severity="INFO")
        print('Now copying into the real table...')
        with db.begin() as cn:
            sql = text("""INSERT INTO notes SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING;""")
            cn.execute(sql)

        try:
            cur.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
            logger.log_struct(
                {
                    "message": 'Dropped temporary table',
                    "severity": 'INFO',
                    "table-name": table_name
                }
            )
        except Exception as e:
            print('Unable to drop a temp table. Does it actually exist?')
            print(str(type(e)))
            logger.log_struct(
                {
                    "message": "Error when dropping temp_notes",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        conn.commit()
    except Exception as e:
        print('Error when processing notes:')
        print(str(type(e)))
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving notes.tsv",
                "severity": "WARNING",
                "exception": str(type(e))
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
    


    ## Get ratings ##
    try:
        object = file_path + '/ratings.tsv'
        table_name = 'temp_ratings_' + date.today().strftime("%Y%m%d")
        df = retrieve_tsv(object)
        df['ratingsId'] = df[['noteId', 'raterParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        # Only keep the top 25% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.75)
        df.drop(df.tail(drop).index, inplace = True)
        logger.log_struct(
            {
                "message": 'Dropped rows from dataframe',
                "original-size": str(size),
                "dropped-rows": str(drop),
                "new-size": str(df.shape[0]),
                "severity": 'INFO',
            }
        )
        print("***")
        print(df)
        print('Now converting dataframe into sql and placing into a temporary table')
        logger.log_struct(
            {
                "message": 'Now converting dataframe into sql and placing into a temporary table',
                "severity": "INFO",
                "object": str(object),
                "table-name": table_name
            }
        )
        df.to_sql(table_name, db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Copying temp_ratings into ratings', severity="INFO")
        with db.begin() as cn:
            sql = text("""INSERT INTO ratings SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING;""")
            cn.execute(sql)
        try:
            cur.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
            logger.log_struct(
                {
                    "message": 'Dropped temporary table',
                    "severity": 'INFO',
                    "table-name": table_name
                }
            )
        except Exception as e:
            print('Unable to drop a temp table. Does it actually exist?')
            print(str(type(e)))
            logger.log_struct(
                {
                    "message": "Error when dropping temp_ratings",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        conn.commit()
    except Exception as e:
        print('Error when getting ratings:')
        print(str(type(e)))
        print(e)
        logger.log_struct(
            {
                "message": "Error when processing ratings.tsv",
                "severity": "WARNING",
                "exception": str(type(e))
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
        object = file_path + '/noteStatusHistory.tsv'
        table_name = 'temp_status_' + date.today().strftime("%Y%m%d")
        df = retrieve_tsv(object)
        df['statusId'] = df[['noteId', 'noteAuthorParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        # Only keep the top 25% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.75)
        df.drop(df.tail(drop).index, inplace = True)
        logger.log_struct(
            {
                "message": 'Dropped rows from dataframe',
                "original-size": str(size),
                "dropped-rows": str(drop),
                "new-size": str(df.shape[0]),
                "severity": 'INFO',
            }
        )
        print("***")
        print(df)
        print('Now converting dataframe into sql and placing in a temporary table')
        logger.log_struct(
            {
                "message": 'Now converting dataframe into sql and placing into a temporary table',
                "severity": "INFO",
                "object": str(object),
                "table-name": table_name
            }
        )
        df.to_sql(table_name, db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Copying temp_status into status_history', severity="INFO")
        with db.begin() as cn:
            sql = text("""INSERT INTO status_history SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING;""")
            cn.execute(sql)
        try:
            cur.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
            logger.log_struct(
                {
                    "message": 'Dropped temporary table',
                    "severity": 'INFO',
                    "table-name": table_name
                }
            )
        except Exception as e:
            print('Unable to drop a temp table. Does it actually exist?')
            print(str(type(e)))
            logger.log_struct(
                {
                    "message": "Error when dropping temp_status",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        conn.commit()
    except Exception as e:
        print('Error when processing noteStatusHistory:')
        print(str(type(e)))
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving noteStatusHistory.tsv",
                "severity": "WARNING",
                "exception": str(type(e))
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
        object = file_path + '/userEnrollmentStatus.tsv'
        table_name = 'temp_enrollment_' + date.today().strftime("%Y%m%d")
        df = retrieve_tsv(object)
        # Participant Ids may be duplicated (because the same user's status may change), so we concatenate with the timestamp to create a primary key
        df['statusId'] = df[['participantId', 'timestampOfLastStateChange']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        # Only keep the top 25% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.75)
        df.drop(df.tail(drop).index, inplace = True)
        logger.log_struct(
            {
                "message": 'Dropped rows from dataframe',
                "original-size": str(size),
                "dropped-rows": str(drop),
                "new-size": str(df.shape[0]),
                "severity": 'INFO',
            }
        )
        print("***")
        print(df)
        print('Now converting dataframe into sql and placing in a temporary table')
        logger.log_struct(
            {
                "message": 'Now converting dataframe into sql and placing into a temporary table',
                "severity": "INFO",
                "object": str(object),
                "table-name": table_name
            }
        )
        df.to_sql(table_name, db, if_exists='replace')

        print('Now copying into the real table...')
        logger.log('Copying temp_userenrollment into enrollment_status', severity="INFO")
        with db.begin() as cn:
            sql = text("""INSERT INTO enrollment_status SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING""")
            cn.execute(sql)
        try:
            cur.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
            logger.log_struct(
                {
                    "message": 'Dropped temporary table',
                    "severity": 'INFO',
                    "table-name": table_name
                }
            )
        except Exception as e:
            print('Unable to drop a temp table. Does it actually exist?')
            print(str(type(e)))
            logger.log_struct(
                {
                    "message": "Error when dropping temp_enrollment",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        conn.commit()
    except Exception as e:
        print('Error when processing userEnrollmentStatus:')
        print(str(type(e)))
        print(e)
        logger.log_struct(
            {
                "message": "Error when retreiving userEnrollmentStatus.tsv",
                "severity": "WARNING",
                "exception": str(type(e))
            })

    
    print('Attempting to Commit any lingering SQL changes')
    logger.log('Attempting to Commit any lingering SQL changes', severity="INFO")
    try:
        conn.commit()
    except Exception as e:
        print('Unable to commit SQL changes. Was anything actually changed?')
        print(str(type(e)))
        logger.log_struct(
            {
                "message": "Unable to commit SQL changes. Was anything actually changed?",
                "severity": "WARNING",
                "exception": str(type(e))
            })


    # close the db connection
    print('Closing the connection')
    logger.log('Closing the db connection', severity="INFO")
    conn.close()



    print('Done!')

if __name__ == "__main__":
    start_time = datetime.now()
    print('FYI: Script started directly as __main__')
    logger.log('Script Execution Started - import-tsv.py', severity="NOTICE")
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...
    end_time = datetime.now()
    total_time = end_time - start_time
    print(f'Total execution was: {total_time}')
    logger.log('Script execution finished', severity="NOTICE")