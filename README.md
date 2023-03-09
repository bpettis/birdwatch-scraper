# birdwatch-scraper
Retrieves publicly accessible data from Twitter's Birdwatch program

## About Birdwatch

Birdwatch aims to create a better informed world, by empowering people on Twitter to collaboratively add helpful notes to Tweets that might be misleading.

Contributors can identify Tweets they believe are misleading, write notes that provide context to the Tweet, and rate the quality of other contributors’ notes. Through consensus from a broad and diverse set of people, our eventual goal is that the most helpful notes will be visible directly on Tweets, available to everyone on Twitter.

See https://twitter.github.io/birdwatch/overview/ for more information

*** There is a possibility that Elon Musk will decide to rename the Birdwatch program to something like "community notes" (https://www.businessinsider.com/musk-renames-birdwatch-community-notes-touts-improving-accuracy-2022-11). With this change, there is a decent chance that the URL schema will also change for these files... _hopefully_ the data structure remains consistent enough.

## Data Downloads

>> All Birdwatch contributions are publicly available on the [Download Data](https://twitter.com/i/birdwatch/download-data) page of the Birdwatch site so that anyone in the US has free access to analyze the data, identify problems, and spot opportunities to make Birdwatch better.

### Data Access

The "Download data" page is supposedly fully publicly accessible, but attempting to load this page in a fresh browser leads to a Twitter log-in page

https://twitter.com/i/birdwatch/download-data

However, the URL structure for the actual data files seems consistent enough:

- Notes data: `https://ton.twimg.com/birdwatch-public-data/2022/11/09/notes/notes-00000.tsv`
- Ratings data: `https://ton.twimg.com/birdwatch-public-data/2022/11/09/noteRatings/ratings-00000.tsv`
- Note status history data: `https://ton.twimg.com/birdwatch-public-data/2022/11/09/noteStatusHistory/noteStatusHistory-00000.tsv`

These URLs are publicly accessible without authentication. Past data uploads appear to be accessible by swapping out the year, month, and dates. File names are consistent

**update: this is only true for recent-ish files. Most everything else returns 404s :/
Looks like I can only get the last month or so

# Project Goals:

## Short Term

1. Download past data from the beginning of the Birdwatch program (January 2021, approx 675 days worth of data)
2. Download new data after it is posted (ongoing, so long as Twitter keeps running Birdwatch)

## Long Term

1. Ingest data from TSV files into a searchable database
2. Analyze trends in Birdwatch participation
3. Conduct distant reading of Birdwatch notes

## Longer Term

1. Publish this processed data for other researchers to use
2. Make a public interface to interact with the database in an easily accessible way

# Notes:

**To-do:** Make the readme a bit more organized and sensible

- On Feb 7 2023 I worked on adding functionality to download another file which is now available on twitter - the "User Enrollment Status History", which is the user "ranking" and its changes
  - As part of this work, I ran into some problems with the prior script's functioning. Namely in terms of the fact that some column names may have changed in the TSV files. 
    - In the notes and noteStatusHistory data, the `noteAuthorParticipantId` is used to store the participantId and not `participantId`
    - In the ratings data `raterParticipantId` is used to store the participant Id.


- The Dockerfile will build an image that can run the scripts - it relies on having the correct information set in the .env file
- Permissions for Google Cloud service account
  - you will need to have a 'credentials.json' key file for a service account which has permission to access Cloud SQL as well as permission to write to Google Cloud Logging

Overriding the date:

in `import-old-tsv.py` you can use the `START_DATE` environment variable to set the earliest date that the script should start with, formatted like so: `2023, 2, 19` or `2022, 12, 25`

in `import-tsv.py` you can use the `DATE_OVERRIDE` environment variable to specify a date _other_ than the current system time. This can be useful if trying to do some testing, and there isn't a birdwatch file for the current day. Set this like so: `2023/02/22`