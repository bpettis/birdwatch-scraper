from datetime import date, timedelta


# some global variables:
start_date = date(2021, 1, 25)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def cloud_function():
    print('Script started in Google Cloud Function')

if __name__ == "__main__":
    print('Script started directly as __main__')

    # make a list of URLs to query

    # for each URL:

    #   query the URL
    #   save the file
    #   <handle errors>
    #   push the file to Google Cloud Storage
    #   delete the file locally

    end_date = date.today()
    for single_date in daterange(start_date, end_date):
        print(single_date.strftime("%Y-%m-%d"))