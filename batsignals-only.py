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

    
    # Read the file
    df = pd.read_csv(path, sep='\t', header=0, compression='zip')

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


    ## Get batSignal (Note Requests) ##
    try:
        object = file_path + '/batSignals.zip'
        table_name = 'note_requests_' + start_date
        df = retrieve_tsv(object)
        df.sort_values(by=['createdAtMillis'], ascending=False, inplace=True)
        # Participant Ids may be duplicated (because the same user's status may change), so we concatenate with the timestamp to create a primary key
        df['statusId'] = df[['userId', 'createdAtMillis']].astype(str).apply(lambda x: ''.join(x), axis=1)
        print(df.info())
        print(df)
        # Only keep the top 10% of the dataframe - we are almost always dealing with duplicated data, so this will improve runtime
        size = df.shape[0]
        drop = int(size * 0.9)
        # drop = int(size - 10) # use a small number when testing - it'll go way faster!
        drop = 0 # Keep
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


        print('Now copying into the real table...')
        logger.log('Copying temp_note_requests into note_requests', severity="INFO")
        sql = 'INSERT INTO note_requests ("statusId", "userId", "tweetId", "createdAtMillis", "sourceLink") SELECT "statusId", "userId", "tweetId", "createdAtMillis", "sourceLink" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)
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
                    "message": "Error when dropping note_requests",
                    "severity": "WARNING",
                    "table-name": table_name,
                    "exception": str(type(e))
                })
        cursor.close()
        connection.commit()
        db.putconn(connection)
    except Exception as e:
        print('Error when processing batSignals:')
        print(str(type(e)))
        message = e.args[0]
        logger.log_struct(
            {
                "message": "Error when retreiving batSignals.tsv",
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