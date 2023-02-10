from datetime import date, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError
from google.cloud import storage
import urllib.request, time, os
import requests
import gzip



# some global variables:
end_date = date.today()

bucket_name = os.environ.get("gcs_bucket_name")
project_id = os.environ.get("GCP_PROJECT")

dates_list = []
url_list = {}

def query_url(url):
    print(f'Querying {url}')
    try:
        r = requests.get(url, allow_redirects=True)
        print(r.status_code)
        if (r.status_code != 200):
            print("Didn't get a HTTP 200 response")
            return 1
        print(r.headers.get('content-type'))
        return r.content
    except Exception as e:
        print('Something went wrong!')
        print(type(e))
        print(e)
        return 1
    # try:
    #     # request.add_header('Accept-Encoding','gzip, deflate')
    #     response = urllib.request.urlopen(url)
    #     code = response.getcode()
    #     print(f'{url} - {code}')
    #     print(response.headers.get_content_charset())
    #     try:
    #         content=gzip.decompress(response.read().decode('utf-8'))
    #         gzip_fd = gzip.GzipFile(fileobj=fd)
    #     except gzip.BadGzipFile as e:
    #         content=response.read().decode('utf-8')
    #     return content
    # except HTTPError as e:
    #     print(f'urllib.error.HTTPError - HTTP Error {e.code} | {e.reason}')
    #     if e.code == 429: # HTTP 429 - too many requests
    #         retry = e.headers['Retry-After'] # Check if the server told us how long to wait before sending the next request
    #         try:
    #             retry = int(retry) # Try converting to an int to check if we got a real number or not
    #         except ValueError:
    #             retry = 30 # We'll use the "Retry-After" value from the headers if present, but otherwise try again after 30 seconds
    #         except TypeError:
    #             retry = 30 # Use 30 seconds if we have a NoneType trying to go into the retry value
    #         print(f'Waiting {retry} seconds before trying the next URL...')
    #         time.sleep(retry)
    #     return 1
    # except ConnectionResetError as e:
    #     print('Got ConnectionResetError - waiting a bit before trying the next URL')
    #     time.sleep(15)
    #     return 1
    # except BrokenPipeError as e:
    #     print('Got BrokenPipeError - waiting a bit before trying the next URL')
    #     time.sleep(15)
    #     return 1
    # except Exception as e:
    #     print(f'Got some other error when attempting to download {url}')
    #     print(type(e))
    #     print(e)
    #     return 1

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

    blob.upload_from_string(contents)

    print(
        f"{destination_blob_name} was uploaded to {bucket_name}."
    )

def main(event_data, context):
    # We have to include event_data and context because these will be passed as arguments when invoked as a Cloud Function
    # and the runtime will freak out if the function only accepts 0 arguments... go figure


    # Our list of dates to check only needs one date, today:
    dates_list.append(date.today().strftime("%Y/%m/%d"))

    # Use those dates to create a list of URLs to then download
    for target_date in dates_list:
        url_list[target_date] = {'notes': '', 'ratings': '', 'noteStatusHistory': '', 'userEnrollmentStatus': ''}
        url_list[target_date]['notes'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/notes/notes-00000.tsv')
        url_list[target_date]['ratings'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteRatings/ratings-00000.tsv')
        url_list[target_date]['noteStatusHistory'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteStatusHistory/noteStatusHistory-00000.tsv')
        url_list[target_date]['userEnrollmentStatus'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/userEnrollment/userEnrollment-00000.tsv')

    for target in url_list:
        # download notes
        data = query_url(url_list[target]['notes'])
        destination_file = target + '/notes.tsv'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')


        # download ratings
        data = query_url(url_list[target]['ratings'])
        destination_file = target + '/ratings.tsv'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')

        # download notes status history
        data = query_url(url_list[target]['noteStatusHistory'])
        destination_file = target + '/noteStatusHistory.tsv'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')

        # get user enrollment status data
        data = query_url(url_list[target]['userEnrollmentStatus'])
        destination_file = target + '/userEnrollmentStatus.tsv'
        if isinstance(data, bytes):
            print(f'Looks like the download worked! Now saving {destination_file} to Google Cloud Storage')
            upload_blob(data, destination_file)
        else:
            print('seems something went wrong. check above for error messages')

    print('Finished!')
    
if __name__ == "__main__":
    print('FYI: Script started directly as __main__')
    main('foo', 'bar') # see note in main() for why we have these filler variables that aren't actually doing anything...