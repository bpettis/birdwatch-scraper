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