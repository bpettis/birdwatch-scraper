from datetime import date, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError
from google.cloud import storage
import google.cloud.logging
import urllib.request, time, os
import requests
import gzip
from dotenv import load_dotenv, find_dotenv



# some global variables:
end_date = date.today()

load_dotenv(find_dotenv()) # load environment variables
bucket_name = os.environ.get("gcs_bucket_name")
project_id = os.environ.get("GCP_PROJECT")
log_name = os.environ.get("LOG_ID")
# Set up Google cloud logging:
log_client = google.cloud.logging.Client(project=project_id)
logger = log_client.logger(name=log_name)

dates_list = []
url_list = {}

def query_url(url):
    print(f'Querying {url}')
    logger.log_struct(
            {
                "message": "Querying URL and attempting to download file",
                "severity": "INFO",
                "url": str(url)
            })
    try:
        r = requests.get(url, allow_redirects=True)
        print(r.status_code)
        if (r.status_code != 200):
            print("Didn't get a HTTP 200 response")
            logger.log_struct(
                {
                    "message": "Didn't get a HTTP 200 response",
                    "severity": "ERROR",
                    "http-status": str(r.status_code),
                })
            return 1
        print(r.headers.get('contnt-type'))
        return r.content
    except Exception as e:
        print('Something went wrong!')
        print(type(e))
        print(e)
        logger.log_struct(
            {
                "message": "General error downloading file",
                "severity": "ERROR",
                "error": str(e)
            })
        return 1

def upload_blob(contents, destination_blob_name):
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents, timeout=300, content_type="application/zip")

    print(
        f"{destination_blob_name} was uploaded to {bucket_name}."
    )
    logger.log_struct(
        {
            "message": "Finished Uploading file to GCS",
            "severity": "INFO",
            "destination_blob_name": str(destination_blob_name)
        })

def main(event_data, context):
    # We have to include event_data and context because these will be passed as arguments when invoked as a Cloud Function
    # and the runtime will freak out if the function only accepts 0 arguments... go figure


    # Our list of dates to check only needs one date, today:
    # dates_list.append(date.today().strftime("%Y/%m/%d"))

    # Check the last 2 days of data:
    for i in range(2):
        dates_list.append((date.today() - timedelta(days=i)).strftime("%Y/%m/%d"))

    # Use those dates to create a list of URLs to then download
    for target_date in dates_list:
        url_list[target_date] = {'notes': '', 'ratings': '', 'noteStatusHistory': '', 'userEnrollmentStatus': ''}
        url_list[target_date]['notes'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/notes/notes-00000.zip')
        url_list[target_date]['ratings'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteRatings/ratings-00000.zip')
        url_list[target_date]['noteStatusHistory'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteStatusHistory/noteStatusHistory-00000.zip')
        url_list[target_date]['userEnrollmentStatus'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/userEnrollment/userEnrollment-00000.zip')
        url_list[target_date]['batSignals'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/batSignals/batSignals-00000.zip')

    logger.log_struct(
        {
            "message": "Created a list of URLs to try and download",
            "severity": "DEBUG",
            "url_list": str(url_list),
            "dates_list": str(dates_list)
        })


    for target in url_list:

        # Download notes
        current_url = url_list[target]['notes']
        data = query_url(current_url)
        


        # This is what the file should be named, if we're being sensible
        destination_file = target + '/notes00000.zip'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print(f'Error when downloading {current_url}. check above for error messages')



        # download ratings - which is what has 10 separate files

        for i in range(20):
            current_url = url_list[target]['ratings'].replace('00000', str(i).zfill(5)) # replace the 00000 with the correct number, padding with zeros if necessary
            # download notes
            data = query_url(current_url)
            destination_file = target + '/ratings' + str(i).zfill(5) + '.zip'
            if isinstance(data, bytes):
                print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
                upload_blob(data, destination_file)
            else:
                print(f'Error when downloading {current_url}. check above for error messages')

        # download notes status history

        current_url = url_list[target]['noteStatusHistory']
        data = query_url(current_url)
        destination_file = target + '/noteStatusHistory' + str(i).zfill(5) + '.zip'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print(f'Error when downloading {current_url}. check above for error messages')



        # get user enrollment status data
        data = query_url(url_list[target]['userEnrollmentStatus'])
        destination_file = target + '/userEnrollmentStatus.zip'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')
            
            
        # get "Note Requests" (bat signals) data
        data = query_url(url_list[target]['batSignals'])
        destination_file = target + '/batSignals.zip'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')

    print('Finished!')
    
if __name__ == "__main__":
    print('FYI: Script started directly as __main__')
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...