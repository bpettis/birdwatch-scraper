from datetime import date, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError
import urllib.request, time


# some global variables:
start_date = date(2021, 1, 25)
start_date = date(2022,11,1)
end_date = date.today()

dates_list = []
url_list = {}

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def query_url(url):
    print(f'Querying {url}')
    try:
        response = urllib.request.urlopen(url)
        code = response.getcode()
        print(f'{url} - {code}')
        return response.read()
    except HTTPError as e:
        print(f'urllib.error.HTTPError - HTTP Error {e.code} | {e.reason}')
        if e.code == 429: # HTTP 429 - too many requests
            retry = e.headers['Retry-After'] # Check if the server told us how long to wait before sending the next request
            try:
                retry = int(retry) # Try converting to an int to check if we got a real number or not
            except ValueError:
                retry = 30 # We'll use the "Retry-After" value from the headers if present, but otherwise try again after 30 seconds
            except TypeError:
                retry = 30 # Use 30 seconds if we have a NoneType trying to go into the retry value
            print(f'Waiting {retry} seconds before trying the next URL...')
            time.sleep(retry)
        return 1
    except ConnectionResetError as e:
        print('Got ConnectionResetError - waiting a bit before trying the next URL')
        time.sleep(15)
        return 1
    except BrokenPipeError as e:
        print('Got BrokenPipeError - waiting a bit before trying the next URL')
        time.sleep(15)
        return 1
    except Exception as e:
        print(f'Got some other error when attempting to download {url}')
        print(e)
        return 1

def cloud_function():
    print('Reached Google Cloud Function Entry Point')

    
    

    # Create a list of dates to check:
    for single_date in daterange(start_date, end_date):
        dates_list.append(single_date.strftime("%Y/%m/%d"))

    # Use those dates to create a list of URLs to then download
    url_counter = 0
    for target_date in dates_list:
        url_list[target_date] = {'notes': '', 'ratings': '', 'noteStatusHistory': ''}
        url_list[target_date]['notes'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/notes/notes-00000.tsv')
        url_list[target_date]['ratings'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteRatings/ratings-00000.tsv')
        url_list[target_date]['noteStatusHistory'] = ('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteStatusHistory/noteStatusHistory-00000.tsv')
        url_counter += 3
    print(f'Created a dictionary containing URLs for {len(url_list)} dates of past data. It contains {str(url_counter)} total URLs')
    # for each URL:
    for target in url_list:
        data = query_url(url_list[target]['notes'])
        if isinstance(data, bytes):
            print('neato the download worked!')
        else:
            print('seems something went wrong. check above for error messages')

        # save the file
        # <handle errors>
        # push the file to Google Cloud Storage
        # delete the file locally
    
if __name__ == "__main__":
    print('FYI: Script started directly as __main__')
    cloud_function()