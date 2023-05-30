from google.cloud import storage
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine, text
from datetime import datetime, date, timedelta
from dotenv import load_dotenv, find_dotenv
import os, sqlalchemy, pg8000, psycopg2
import google.cloud.logging
import socket
from psycopg2 import pool

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
start_date = os.environ.get("START_DATE", "2023, 5, 15")
start_date = datetime.strptime(start_date, '%Y, %m, %d')
end_date = datetime.today()
dates_list = []

# Get DB info from the environment
db_host = os.environ.get("DB_HOST")
db_user = os.environ.get("DB_USER")
db_name = os.environ.get("DB_NAME")
db_password = os.environ.get("DB_PASS")

# Set up Google cloud logging:
log_client = google.cloud.logging.Client(project=project_id)
logger = log_client.logger(name='import-old-tsv')

# [START cloud_sql_postgres_sqlalchemy_connect_connector]
# From https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/cloud-sql/postgres/sqlalchemy/connect_connector.py
# thx Google ;)



def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


## postgres connection:
def connection_pool():
    try:
        pool = psycopg2.pool.SimpleConnectionPool(1, 10,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432",
            database=db_name)
        return pool
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        quit()

def connection_engine():
    conn_string = 'postgresql://' + db_user + ':' + db_password + '@' + db_host + '/' + db_name
    db_engine = create_engine(conn_string)
    connection_engine = db_engine.connect()
    return connection_engine

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
    print(f'Started Execution, with an initial date of: {start_date}')
    load_dotenv() # load environment variables
    
    # Set up a db connection pool
    db = connection_pool()

    # Set up a db engine
    engine = connection_engine()

    

    # Create a list of dates to check:
    for single_date in daterange(start_date, end_date):
        dates_list.append(single_date.strftime("%Y/%m/%d"))

    for current_date in dates_list:
        file_path = current_date

        ## Get notes ##
        logger.log_struct(
            {
                "message": "Retrieving notes.tsv",
                "severity": "INFO",
                "current-date": str(file_path)
            })
        object = file_path + '/notes.tsv'
        table_name = 'temp_notes_' + date.today().strftime("%Y%m%d")
        try: 
            df = retrieve_tsv(object)
            print(df.info())
            print(df)

            # # Insert data from that file into the db:
            logger.log_struct(
                {
                    "message": 'Now converting dataframe into sql and placing into a temporary table',
                    "severity": "INFO",
                    "object": str(object),
                    "table-name": table_name
                }
            )
            df.to_sql(table_name, engine, if_exists='replace')
            logger.log('Copying temp_notes into the notes table', severity="INFO")
            print('Now copying into the real table...')
            connection = db.getconn()
            cursor = connection.cursor()
            sql = 'INSERT INTO notes ("noteId", "createdAtMillis", "tweetId", "classification", "believable", "harmful", "validationDifficulty", "misleadingOther", "misleadingFactualError", "misleadingManipulatedMedia", "misleadingOutdatedInformation", "misleadingMissingImportantContext", "misleadingUnverifiedClaimAsFact", "misleadingSatire", "notMisleadingOther", "notMisleadingFactuallyCorrect", "notMisleadingOutdatedButNotWhenWritten", "notMisleadingClearlySatire", "notMisleadingPersonalOpinion", "trustworthySources", "summary", "noteAuthorParticipantId" ) SELECT "noteId", "createdAtMillis", "tweetId", "classification", "believable", "harmful", "validationDifficulty", "misleadingOther", "misleadingFactualError", "misleadingManipulatedMedia", "misleadingOutdatedInformation", "misleadingMissingImportantContext", "misleadingUnverifiedClaimAsFact", "misleadingSatire", "notMisleadingOther", "notMisleadingFactuallyCorrect", "notMisleadingOutdatedButNotWhenWritten", "notMisleadingClearlySatire", "notMisleadingPersonalOpinion", "trustworthySources", "summary", "noteAuthorParticipantId" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)
            cursor.execute(sql)
            try:
                cursor.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
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
            cursor.close()
            connection.commit()
            db.putconn(connection)
        except Exception as e:
            print('Error when getting notes:')
            print(str(type(e)))
            print(e)
            logger.log_struct(
                {
                    "message": "Error when retreiving notes.tsv",
                    "severity": "WARNING",
                    "exception": str(type(e))
                })


        

        # print('Now inserting data from table_temp into notes - and skipping duplicates')
        # cur.execute("""Insert into notes select * From table_temp ON CONFLICT DO NOTHING;""");

        # print('Now dropping the temporary table')
        # cur.execute("""DROP TABLE table_temp CASCADE;""");  # You can drop if you want to but the replace option in to_sql will drop and recreate the table
        # conn.commit()

        ## Get ratings ##
        logger.log_struct(
            {
                "message": "Retrieving ratings.tsv",
                "severity": "INFO",
                "current-date": str(file_path)
            })
        object = file_path + '/ratings.tsv'
        table_name = 'temp_ratings_' + date.today().strftime("%Y%m%d")
        print(f'Searching for {object}')
        try:
            df = retrieve_tsv(object)
            df['ratingsId'] = df[['noteId', 'raterParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
            print(df.info())
            print(df)
            logger.log_struct(
                {
                    "message": 'Now converting dataframe into sql and placing into a temporary table',
                    "severity": "INFO",
                    "object": str(object),
                    "table-name": table_name
                }
            )
            print('Now converting dataframe into sql and placing into a temporary table')
            df.to_sql(table_name, engine, if_exists='replace')
            logger.log('Copying temp_ratings into ratings', severity="INFO")

            print('Now copying into the real table...')
            connection = db.getconn()
            cursor = connection.cursor()
            sql = 'INSERT INTO ratings ("noteId", "createdAtMillis", "version", "agree", "disagree", "helpful", "notHelpful", "helpfulnessLevel", "helpfulOther", "helpfulInformative", "helpfulClear", "helpfulEmpathetic", "helpfulGoodSources", "helpfulUniqueContext", "helpfulAddressesClaim", "helpfulImportantContext", "helpfulUnbiasedLanguage", "notHelpfulOther", "notHelpfulIncorrect", "notHelpfulSourcesMissingOrUnreliable", "notHelpfulOpinionSpeculationOrBias", "notHelpfulMissingKeyPoints", "notHelpfulOutdated", "notHelpfulHardToUnderstand", "notHelpfulArgumentativeOrBiased", "notHelpfulOffTopic", "notHelpfulSpamHarassmentOrAbuse", "notHelpfulIrrelevantSources", "notHelpfulOpinionSpeculation", "notHelpfulNoteNotNeeded", "ratingsId", "raterParticipantId") SELECT "noteId", "createdAtMillis", "version", "agree", "disagree", "helpful", "notHelpful", "helpfulnessLevel", "helpfulOther", "helpfulInformative", "helpfulClear", "helpfulEmpathetic", "helpfulGoodSources", "helpfulUniqueContext", "helpfulAddressesClaim", "helpfulImportantContext", "helpfulUnbiasedLanguage", "notHelpfulOther", "notHelpfulIncorrect", "notHelpfulSourcesMissingOrUnreliable", "notHelpfulOpinionSpeculationOrBias", "notHelpfulMissingKeyPoints", "notHelpfulOutdated", "notHelpfulHardToUnderstand", "notHelpfulArgumentativeOrBiased", "notHelpfulOffTopic", "notHelpfulSpamHarassmentOrAbuse", "notHelpfulIrrelevantSources", "notHelpfulOpinionSpeculation", "notHelpfulNoteNotNeeded", "ratingsId", "raterParticipantId" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)
            cursor.execute(sql)
            try:
                cursor.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
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
            cursor.close()
            connection.commit()
            db.putconn(connection)
        except Exception as e:
            print('Error when getting ratings:')
            print(str(type(e)))
            print(e)
            logger.log_struct(
                {
                    "message": "Error when retreiving ratings.tsv",
                    "severity": "WARNING",
                    "exception": str(type(e))
                })




        ## Get noteStatusHistory ##
        logger.log_struct(
            {
                "message": "Retrieving noteStatusHistory.tsv",
                "severity": "INFO",
                "current-date": str(file_path)
            })
        object = file_path + '/noteStatusHistory.tsv'
        table_name = 'temp_status_' + date.today().strftime("%Y%m%d")
        print(f'Searching for {object}')
        try:
            df = retrieve_tsv(object)
            try:
                df['statusId'] = df[['noteId', 'noteAuthorParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
            except:
                try:
                    df['statusId'] = df[['noteId', 'participantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
                except:
                    df['statusId'] = 'IDERROR' + datetime.now().strftime('%s')

            # Coax this column into being an actual number, and replace any NaN values with 0
            df['timestampMillisOfStatusLock'] = pd.to_numeric(df['timestampMillisOfStatusLock'], errors='coerce').fillna(0).astype(int)

            print(df.info())
            print(df)
            logger.log_struct(
                {
                    "message": 'Now converting dataframe into sql and placing into a temporary table',
                    "severity": "INFO",
                    "object": str(object),
                    "table-name": table_name
                }
            )
            print('Now converting dataframe into sql and placing in a temporary table')
            df.to_sql(table_name, engine, if_exists='replace')

            # After moving data to the temporary table, attempt to force the column to be the correct type:
            sql = 'ALTER TABLE {0} ALTER COLUMN "timestampMillisOfStatusLock" TYPE BIGINT;'.format(table_name)
            print(f'Attempting to run SQL statement: {str(sql)}')
            logger.log_struct(
                {
                    "message": 'Running SQL statement to convert column datatype',
                    "severity": 'INFO',
                    "table-name": table_name,
                    "column-name": 'timestampMillisOfStatusLock',
                    "sql": str(sql)
                }
            )
            cursor.execute(sql)

            logger.log('Copying temp_status into status_history', severity="INFO")
            print('Now copying into the real table...')
            connection = db.getconn()
            cursor = connection.cursor()
            # Manually specify which columns to insert so that we can *force* "timestampMillisOfStatusLock" to be cast as BIGINT when inserting into the primary table
            sql = 'INSERT INTO status_history ("noteId", "noteAuthorParticipantId", "createdAtMillis", "timestampMillisOfFirstNonNMRStatus", "firstNonNMRStatus", "timestampMillisOfCurrentStatus", "currentStatus", "timestampMillisOfLatestNonNMRStatus", "mostRecentNonNMRStatus", "timestampMillisOfStatusLock", "lockedStatus", "timestampMillisOfRetroLock", "statusId") SELECT "noteId", "noteAuthorParticipantId", "createdAtMillis", "timestampMillisOfFirstNonNMRStatus", "firstNonNMRStatus", "timestampMillisOfCurrentStatus", "currentStatus", "timestampMillisOfLatestNonNMRStatus", "mostRecentNonNMRStatus", "timestampMillisOfStatusLock"::BIGINT, "lockedStatus", "timestampMillisOfRetroLock", "statusId" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)

            cursor.execute(sql)
            try:
                cursor.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
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
            cursor.close()
            connection.commit()
            db.putconn(connection)


        except Exception as e:
            print('Error when getting noteStatusHisotyr:')
            print(str(type(e)))
            print(e)
            logger.log_struct(
                {
                    "message": "Error when retreiving notesStatusHistory.tsv",
                    "severity": "WARNING",
                    "exception": str(type(e))
                })
            
        



        ## Get userEnrollmentStatus ##
        logger.log_struct(
            {
                "message": "Retrieving userEnrollmentStatus.tsv",
                "severity": "INFO",
                "current-date": str(file_path)
            })
        object = file_path + '/userEnrollmentStatus.tsv'
        table_name = 'temp_enrollment_' + date.today().strftime("%Y%m%d")
        try:
            df = retrieve_tsv(object)
            # Participant Ids may be duplicated (because the same user's status may change), so we concatenate with the timestamp to create a primary key
            df['statusId'] = df[['participantId', 'timestampOfLastStateChange']].astype(str).apply(lambda x: ''.join(x), axis=1)
            print(df.info())
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
            df.to_sql(table_name, engine, if_exists='replace')

            # Some older data is likely to not include the modelPopulation value, so we add that column if it's not present. It will contain null data, but we add it just in case.
            # sql = text("""INSERT INTO enrollment_status SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING""")
            sql = 'ALTER TABLE {0} ADD COLUMN IF NOT EXISTS "modelingPopulation" TEXT;'.format(table_name)
            cursor.execute(sql)

            print('Now copying into the real table...')
            logger.log('Copying temp_userenrollment into enrollment_status', severity="INFO")
            connection = db.getconn()
            cursor = connection.cursor()
            sql = 'INSERT INTO enrollment_status ("participantId", "enrollmentState", "successfulRatingNeededToEarnIn", "timestampOfLastStateChange", "timestampOfLastEarnOut", "modelingPopulation", "statusId") SELECT "participantId", "enrollmentState", "successfulRatingNeededToEarnIn", "timestampOfLastStateChange", "timestampOfLastEarnOut", "modelingPopulation", "statusId" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)
            cursor.execute(sql)
            try:
                cursor.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")
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
            cursor.close()
            connection.commit()
            db.putconn(connection)
        except Exception as e:
            print('Error when getting enrollment_status:')
            print(str(type(e)))
            print(e)
            logger.log_struct(
                {
                    "message": "Error when retreiving enrollmentStatus.tsv",
                    "severity": "WARNING",
                    "exception": str(type(e))
                })

            
    # close the db engine:
    if engine:
        engine.close()

    # close the db connection pool:
    if db:
        db.closeall
        print("PostgreSQL connection pool is closed")
    
    print('Done!')

if __name__ == "__main__":
    start_time = datetime.now()
    print('FYI: Script started directly as __main__')
    logger.log_struct(
            {
                "message": "Script Execution Started - import-old-tsv.py",
                "severity": "INFO",
                "start-date": str(start_date),
                "hostname": str(socket.gethostname())
            })
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...
    end_time = datetime.now()
    total_time = end_time - start_time
    print(f'Total execution was: {total_time}')
    logger.log_struct(
            {
                "message": "Script Execution Finihed - import-old-tsv.py",
                "severity": "INFO",
                "total-time": str(total_time)
            })