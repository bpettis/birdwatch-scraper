from google.cloud import storage
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from google.cloud.sql.connector import Connector, IPTypes
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
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
bucket_name = os.environ.get("gcs_bucket_name")
project_id = os.environ.get("GCP_PROJECT")
start_date = date(2022, 10, 15)
end_date = date.today()
dates_list = []

# [START cloud_sql_postgres_sqlalchemy_connect_connector]
# From https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/cloud-sql/postgres/sqlalchemy/connect_connector.py
# thx Google ;)



def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

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
    load_dotenv() # load environment variables
    
    # Set up a db connection pool
    db = connect_with_connector()
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        conn = db.raw_connection()
        cur = conn.cursor()
        print('db connection seems to have worked')
    except:
        print('db connection failure')
        quit()

    

    # Create a list of dates to check:
    for single_date in daterange(start_date, end_date):
        dates_list.append(single_date.strftime("%Y/%m/%d"))

    for current_date in dates_list:
        file_path = current_date

        ## Get notes ##
        object = file_path + '/notes.tsv'
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
        print(f'Searching for {object}')
        try: 
            df = retrieve_tsv(object)
            print(df.info())
            print(df)
        except:
            print('Unable to find that TSV file. Skipping')
            continue

        # # Insert data from that file into the db:
        print('Now converting dataframe into sql and placing into a temporary table')
        df.to_sql('temp_notes', db, if_exists='replace')

        print('Now copying into the real table...')
        with db.begin() as cn:
            sql = """INSERT INTO notes
                    SELECT *
                    FROM temp_notes
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()

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
        object = file_path + '/ratings.tsv'
        print(f'Searching for {object}')
        try:
            df = retrieve_tsv(object)
            df['ratingsId'] = df[['noteId', 'participantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
            print(df.info())
            print(df)
        except:
            print('Unable to find that TSV file. Skipping')
            continue
        print('Now converting dataframe into sql and placing into a temporary table')
        df.to_sql('temp_ratings', db, if_exists='replace')

        print('Now copying into the real table...')
        with db.begin() as cn:
            sql = """INSERT INTO ratings
                    SELECT *
                    FROM temp_ratings
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()

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
        object = file_path + '/noteStatusHistory.tsv'
        print(f'Searching for {object}')
        try:
            df = retrieve_tsv(object)
            df['statusId'] = df[['noteId', 'participantId']].astype(str).apply(lambda x: ''.join(x), axis=1)
            print(df.info())
            print(df)
        except:
            print('Unable to find that TSV file. Skipping')
            continue
        print('Now converting dataframe into sql and placing in a temporary table')
        df.to_sql('temp_status', db, if_exists='replace')

        print('Now copying into the real table...')
        with db.begin() as cn:
            sql = """INSERT INTO status_history
                    SELECT *
                    FROM temp_status
                    ON CONFLICT DO NOTHING"""
            cn.execute(sql)
        conn.commit()

        # Clean up temp tables
        print('Now deleting temporary tables!')
        cur.execute("""DROP TABLE temp_notes CASCADE;""");
        cur.execute("""DROP TABLE temp_ratings CASCADE;""");
        cur.execute("""DROP TABLE temp_status CASCADE;""");
        conn.commit()

        # close the db connection
        conn.close()
        print(f'Finished importing data from {current_date}')


    
    print('Done!')

if __name__ == "__main__":
    start_time = datetime.now()
    print('FYI: Script started directly as __main__')
    
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...
    end_time = datetime.now()
    total_time = end_time - start_time
    print(f'Total execution was: {total_time}')