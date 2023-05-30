Birdwatch Downloader - authentication issue

I had been getting several notifications about the parser script failing every day for the past few days. I didn't think much of this, because there had been times in the past where Twitter had not posted a file on a certain day—and I figured that the same may have been going on here. When I noticed it happening _several_ days in a row however, I started to think that maybe something had changed on Twitter's end. For example, they may have changed the URL structure for the TSV files.

But when I checked their site, everything seemed to be the same. The scripts should be working as expected. So it wasn't matter of the parser script failing, but rather that the download script was not correctly downloading the files. 

I tested running the script using the extact crontab entry:
`/usr/bin/python3 /home/bpettis/birdwatch-scraper/download-new.py` 

And the script starts up okay, but then fails once it tries to upload the downloaded TSV file into GCS. As it turns out, my credentials had expired - and the script was _not_ correctly getting the authentication keys for the GCS service account. So basically it just wasn't allowed to upload and was quietly failing.

I reworked the crontab entry to specify the necessary environmental variables:

`0 6 * * * GOOGLE_APPLICATION_CREDENTIALS="/home/bpettis/birdwatch-scraper/keys/cloud-storage-admin.json" GCP_PROJECT="leafy-sanctuary-368121" gcs_bucket_name="birdwatch-scraper_public-data" /usr/bin/python3 /home/bpettis/birdwatch-scraper/download-new.py`

When testing this, I also found that at times the upload would still fail due to the connection timing out. I ended up setting a pretty generous timeout limit: up to 30 minutes for each file. While I doubt this will be necessary, I believe it strikes a good balance between giving enough time for the script to run, but also not introducing the risk of hanging the server with infinitely many stalled GCS uploads. This would be bad news not only for my computer, but for my wallet as well.

I mentioned above that the downlaod script was quietly failing. I had thought that I had added cloud logging to the download scripts, but evidently this is not the case. So I have no good way of checking its progress without manually SSHing into the server and poking around. In the coming weeks, I'd like to add some basic cloud logging to report the HTTP status code of each file each day - simply listing whether a file was downloaded (200 OK) or if it was a 404 Not Found will help me track the project status.