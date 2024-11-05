from google.cloud import storage
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
import google.cloud.logging
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os, sqlalchemy, pg8000, socket, psycopg2
from psycopg2 import pool
import traceback

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
start_date = os.environ.get("DATE_OVERRIDE", date.today().strftime("%Y%m%d")).replace("/", "") # Get the DATE_OVERRIDE environment variable, or use today's date if not present. If the environment variable has forward slashes, remove those.

# Get DB info from the environment
db_host = os.environ.get("DB_HOST")
db_user = os.environ.get("DB_USER")
db_name = os.environ.get("DB_NAME")
db_password = os.environ.get("DB_PASS")


# Set up Google cloud logging:
log_client = google.cloud.logging.Client(project=project_id)
logger = log_client.logger(name=log_name)


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
    print('Started Execution')
    
    
    # Set up a db connection pool
    db = connection_pool()

    # Set up a db engine
    engine = connection_engine()


    # Get the most recent downloaded file
    #   (with error handling for if a file is missing for whatever reason)
    file_path = os.environ.get("DATE_OVERRIDE", date.today().strftime("%Y/%m/%d"))

    ## Get notes ##
    try:
        
        logger.log_struct(
            {

                "message": "Retrieving TSV Files and loading into Pandas dataframe. This may take a while since this can be a large file",
                "severity": "DEBUG",
                "gcs-path-prefix": str(file_path)
            })

        object = file_path + '/notes.tsv'

        # This is what the file *should* be called, from here on out - but there may be some that were incorrectly named notes000004.tsv
        object = file_path + '/notes00000.tsv'

        # Temporary fix to try getting to old name:
        object = file_path + '/notes00004.tsv'

        logger.log_struct(
            {
                "message": 'Built object name',
                "object-name": str(object),
                "severity": 'DEBUG',
            }
        )

        table_name = 'temp_notes_' + start_date
        df = retrieve_tsv(object)
        df.sort_values(by=['createdAtMillis'], ascending=False, inplace=True)
        print(df.info())
        print(df)
        # Only keep the top 10% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.9)
        # drop = int(size - 10) # use a small number when testing - it'll go way faster!
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

        df.to_sql(table_name, engine, if_exists='replace')
        engine.commit()
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
            message = e.args[0]
            logger.log_struct(
                {
                    "message": "Error when dropping temp_notes",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e)),
                    "error": message
                })
        cursor.close()
        connection.commit()
        db.putconn(connection)
    except Exception as e:
        print('Error when processing notes:')
        print(str(type(e)))
        print(e)
        print(traceback.format_exc())
        message = e.args[0]
        logger.log_struct(
            {
                "message": "Error when retreiving notes.tsv",
                "severity": "WARNING",
                "exception": str(type(e)),
                "error": message
            })

    ## Get ratings ##
    try:

        logger.log_struct(
            {
                "message": "Retrieving TSV Files for ratings and loading into Pandas dataframe. This will probably take a LONG time",
                "severity": "DEBUG",
                "gcs-path-prefix": str(file_path)
            })

        # We are now downloading potentially up to 10 TSV files, which we need to concatenate into a single dataframe
        mega_df = pd.DataFrame() # Create an empty dataframe
        for i in range(10):
            object = file_path + '/ratings' + str(i).zfill(5) + '.tsv'
            try:
                df = retrieve_tsv(object)
                mega_df = pd.concat([mega_df, df], ignore_index=True)
            except Exception as e:
                print('File does not exist')
                print(str(type(e)))
                logger.log_struct(
                    {
                        "message": "File does not exist",
                        "severity": "WARNING",
                        "object": str(object),
                        "exception": str(type(e))
                    })
                continue

        table_name = 'temp_ratings_' + start_date
        mega_df.sort_values(by=['createdAtMillis'], ascending=False, inplace=True)
        mega_df['ratingsId'] = mega_df[['noteId', 'raterParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(mega_df.info())
        print(mega_df)
        # Only keep the top 10% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = mega_df.shape[0]
        drop = int(size * 0.9)
        # drop = int(size - 10) # use a small number when testing - it'll go way faster!
        mega_df.drop(mega_df.tail(drop).index, inplace = True)
        logger.log_struct(
            {
                "message": 'Dropped rows from dataframe',
                "original-size": str(size),
                "dropped-rows": str(drop),
                "new-size": str(mega_df.shape[0]),
                "severity": 'INFO',
            }
        )
        print("***")
        print(mega_df)
        print('Now converting dataframe into sql and placing into a temporary table')
        logger.log_struct(
            {
                "message": 'Now converting dataframe into sql and placing into a temporary table',
                "severity": "INFO",
                "object": str(object),
                "table-name": table_name
            }
        )
        mega_df.to_sql(table_name, engine, if_exists='replace')
        engine.commit()

        print('Now copying into the real table...')
        logger.log('Copying temp_ratings into ratings', severity="INFO")
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
            message = e.args[0]
            logger.log_struct(
                {
                    "message": "Error when dropping temp_ratings",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e)),
                    "error": message
                })
        cursor.close()
        connection.commit()
        db.putconn(connection)
    except Exception as e:
        print('Error when getting ratings:')
        print(str(type(e)))
        message = e.args[0]
        logger.log_struct(
            {
                "message": "Error when processing ratings.tsv",
                "severity": "WARNING",
                "exception": str(type(e)),
                "error": message
            })

    ## Get noteStatusHistory ##
    try:
        object = file_path + '/noteStatusHistory.tsv'
        table_name = 'temp_status_' + start_date
        df = retrieve_tsv(object)
        try:
            df['statusId'] = df[['noteId', 'noteAuthorParticipantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
        except:
            try:
                df['statusId'] = df[['noteId', 'participantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
            except:
                df['statusId'] = 'IDERROR' + datetime.now().strftime('%s')

        print(df.info())
        print(df)
        df.sort_values(by=['createdAtMillis'], ascending=False, inplace=True)
        # Only keep the top 10% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.9)
        # drop = int(size - 10) # use a small number when testing - it'll go way faster!
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
        # Coax this column into being an actual number, and replace any NaN values with 0
        df['timestampMillisOfStatusLock'] = pd.to_numeric(df['timestampMillisOfStatusLock'], errors='coerce').fillna(0).astype(int)

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
        engine.commit()


        # After moving data to the temporary table, attempt to force the column to be the correct type:
        connection = db.getconn()
        cursor = connection.cursor()
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
        cursor.close()
        connection.commit()
        db.putconn(connection)


        print('Now copying into the real table...')
        logger.log('Copying temp_status into status_history', severity="INFO")
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
                    "message": "Error when dropping temp_status",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        cursor.close()
        connection.commit()
        db.putconn(connection)
    except Exception as e:
        print('Error when processing noteStatusHistory:')
        print(str(type(e)))
        message = e.args[0]
        logger.log_struct(
            {
                "message": "Error when retreiving noteStatusHistory.tsv",
                "severity": "WARNING",
                "exception": str(type(e)),
                "error": message
            })

    ## Get userEnrollmentStatus ##
    try:
        object = file_path + '/userEnrollmentStatus.tsv'
        table_name = 'temp_enrollment_' + start_date
        df = retrieve_tsv(object)
        df.sort_values(by=['timestampOfLastStateChange'], ascending=False, inplace=True)
        # Participant Ids may be duplicated (because the same user's status may change), so we concatenate with the timestamp to create a primary key
        df['statusId'] = df[['participantId', 'timestampOfLastStateChange']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        # Only keep the top 10% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.9)
        # drop = int(size - 10) # use a small number when testing - it'll go way faster!
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
        df.to_sql(table_name, engine, if_exists='replace')
        engine.commit()

        # Some older data is likely to not include the modelPopulation value, so we add that column if it's not present. It will contain null data, but we add it just in case.
        connection = db.getconn()
        cursor = connection.cursor()
        # sql = text("""INSERT INTO enrollment_status SELECT * FROM """ + table_name + """ ON CONFLICT DO NOTHING""")
        sql = 'ALTER TABLE {0} ADD COLUMN IF NOT EXISTS "modelingPopulation" TEXT;'.format(table_name)
        cursor.execute(sql)

        print('Now copying into the real table...')
        logger.log('Copying temp_userenrollment into enrollment_status', severity="INFO")
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
                    "message": "Error when dropping temp_enrollment",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        cursor.close()
        connection.commit()
        db.putconn(connection)
    except Exception as e:
        print('Error when processing userEnrollmentStatus:')
        print(str(type(e)))
        message = e.args[0]
        logger.log_struct(
            {
                "message": "Error when retreiving userEnrollmentStatus.tsv",
                "severity": "WARNING",
                "exception": str(type(e)),
                "error": message
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
            "message": "Script Execution Started - import-tsv.py",
            "severity": "NOTICE",
            "hostname": str(socket.gethostname()),
            "parsing-files-from": start_date
        })
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...
    end_time = datetime.now()
    total_time = end_time - start_time
    print(f'Total execution was: {total_time}')
    logger.log('Script execution finished', severity="NOTICE")
    logger.log_struct(
        {
            "message": "Script Execution finished - import-tsv.py",
            "severity": "INFO",
            "total-time": str(total_time),
            "hostname": str(socket.gethostname())
        })