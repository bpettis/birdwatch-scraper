from datetime import date, timedelta


# some global variables:
start_date = date(2021, 1, 25)
end_date = date.today()

dates_list = []
url_list = []

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def cloud_function():
    print('Reached Google Cloud Function Entry Point')
    # make a list of URLs to query

    # for each URL:

    #   query the URL
    #   save the file
    #   <handle errors>
    #   push the file to Google Cloud Storage
    #   delete the file locally

    # Create a list of dates to check:
    for single_date in daterange(start_date, end_date):
        dates_list.append(single_date.strftime("%Y/%m/%d"))

    # Use those dates to create a list of URLs to then download
    for target_date in dates_list:
        url_list.append('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/notes/notes-00000.tsv')
        url_list.append('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteRatings/ratings-00000.tsv')
        url_list.append('https://ton.twimg.com/birdwatch-public-data/' + target_date + '/noteStatusHistory/noteStatusHistory-00000.tsv')

    print(f'Created a list of {len(url_list)} URLs')

if __name__ == "__main__":
    print('FYI: Script started directly as __main__')
    cloud_function()