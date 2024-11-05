# birdwatch-scraper
Retrieves publicly accessible data from Twitter's Birdwatch program. 

These scripts download the files that Twitter publishes at https://twitter.com/i/birdwatch/download-data and saves them into a Google Cloud Storage bucket.

The provided Dockerfile creates a container to parse the downloaded files and insert the data into a Postgres database.

## Configuration

Please be sure to create a `.env` file in the project root to provide the following information:

```
gcs_bucket_name='name-of-the-gcs-bucket'
gcs_project_id='id-of-the-google-cloud-project'
GCP_PROJECT='id-of-the-google-cloud-project'

DB_USER='username'
DB_PASS='password'
DB_NAME='databasename'

LOG_ID='name-for-the-log-in-google-cloud-logger'
```

Please create a Google Cloud service account with the following permissions:

- Cloud Storage Object Admin
- Cloud Logging Admin

Create a key JSON file for that account, and place it in `./keys/credentials.json`

## Installation

Install python packages:

```
pip3 install -r requirements.txt
```

Build Docker container:

```
docker build -t birdwatch-importer .
```

Run Docker container:

```
docker run --name birdwatch-importer birdwatch-importer
```

## Scheduling

I recommmend using `cron` to schedule the scripts to run on a regular basis. I was previously this schedule to download new data once per day:

```
0 12 * * * GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json" GCP_PROJECT="google-cloud-project-id" gcs_bucket_name="google-cloud-bucket-name" /usr/bin/python3 /path/to/download-new.py
```

I now use Google Cloud Scheduler to run this script as a Google Cloud Function. Just copy the contents of `download-new.py` and `requirements.py` into the source code for a function running Python. Use Cloud Scheduler to run the task every day. This will run much more quickly because it can save directly into Google Cloud Storage, rather than having to upload files over the public internet.

I use this schedule to run the parser container in docker every day. I use bash substitution to provide each container with a unique name for when it is started:

```
30 12 * * * /usr/bin/docker run --name "birdwatch-parser-$(/usr/bin/date +\%s)" --network host --rm -d birdwatch-parser
```

Just make sure that you have built the container locally before running this.

## Getting participant ids

The `participant-ids.sql` file contains some queries that will determine a list of participant IDs (don't worry - these are all anonymized and de-identified) from the notes, ratings, and enrollment_status tables.

This need to run periodically to update the list of users/participants.

I used `pg_cron` to automate these tasks. See https://github.com/citusdata/pg_cron

After installing and configuring the extension, I set up the jobs. For example:

```
SELECT cron.schedule('Birdwatch Participant IDs from enrollment_status', '30 3 * * *', 'INSERT INTO participants ("participantId", "created_at", "updated_at") (SELECT DISTINCT "participantId", LOCALTIMESTAMP, LOCALTIMESTAMP FROM enrollment_status) ON CONFLICT DO NOTHING');
```

And then just made sure that everything was running on the correct database:

```
UPDATE cron.job SET DATABASE = 'birdwatch'; 
```

---

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

These URLs are publicly accessible without authentication. Past data uploads appear to be accessible by swapping out the year, month, and dates. However, this is only true for recent-ish files. Most everything else returns 404s :/
Looks like I can only get the last month or so

# Project Goals:

## Short Term

- [X] Download past data from the beginning of the Birdwatch program (January 2021, approx 675 days worth of data)
- [X] Download new data after it is posted (ongoing, so long as Twitter keeps running Birdwatch)

## Long Term

- [X] Ingest data from TSV files into a searchable database
- [ ] Improve database and search efficiency
- [ ] Analyze trends in Birdwatch participation
- [ ] Conduct distant reading of Birdwatch notes
- [ ] Save a list of Birdwatch User IDs and add ability to filter notes by author

## Longer Term

- [X] Publish this processed data for other researchers to use
- [X] Make a public interface to interact with the database in an easily accessible way
- [ ] Consider options for downloading Tweet data

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

<h5>A few notes:</h5>
<p>In the coming weeks and months, I plan to do some more organization and more coherent write-up of changes and documentation. But in the meantime, here are some notes to myself (and to the world). </p>
<ul>
    <li>2023-03-11 - the noteStatusHistory.tsv file parsing was failing due to a type conflict in the columns. I reworked the scripts to better handle inserting that data into the main table from the temp table</li>
    <li>2023-03-15 - I think that Twitter quietly added a new column to the data being provded in the userEnrollmentStatus.tsv files, a string for 'modelingPopulation' to indicate whether a user is being rated via the "CORE" or "EXPANSION" model. I'm not entirely sure what this is used for, and what the difference bewteen these models is</li>
    <li>Looks like Twitter also has some scripts that match their internal processes for processing the raw data files and producing an aggregate file that matches what is in production (https://communitynotes.twitter.com/guide/en/under-the-hood/note-ranking-code.html) - I plan to implement my own scheduled process to run this script, in addition to what's already being collected</li>
    <li>2023-05-29 - I am no longer using Google Cloud SQL to host the Birdwatch database, and have migrated to a locally hosted postgreSQL server instead. The website you are currently viewing has been migrated there as well. This provides faster access, more efficient ingest of new data, and lower hosting costs. However, it is slightly more volatile because I am reliant on a residential internet connection and residential power service.</li>
</ul>