# Migrating from Google Cloud SQL on On Premises postgreSQL server

I had already been seeing increasing costs as the site was used more and more.
Plus it was _very_ slow - I had configured the SQL server using the lowest-tier options. But it took pages ~30 seconds to load. The import process was very slow. And I was realizing that doing any meaningful work to query this database was going to take a long time (~19 seconds per simple query). This is not feasible.

Bumping up the server specs in the cloud could improve performance. But might increase my costs from ~$25/month to $100/month - _only_ for the SQL server.

So I decided to try migrating to a locally hosted solution

## Creating the db:

First create a postgres User who will own the db. I'll go ahead and make this match the online name to avoid permissions conflicts

Connect to the server: `sudo -u postgres psql`
Then run the command to create a user: `CREATE USER birdwatch_user WITH PASSWORD '<PASSWORD HERE>' CREATEDB;`
(We specify `CREATEDB` so the user has that permission, though I think it's not actually needed since it's the `postgres` user that creates the database later on)

I test the new User by connecting with `psql -U birdwatch_user -h localhost postgres` 

Next create the database

`sudo -u postgres createdb -O birdwatch_user birdwatch` 

The `-O` option specifies the user that will own the new database

## Import the data

I had an older dump back from March to test this out with. It was an `.sql` file

I imported it using: `psql -U birdwatch_user -h localhost birdwatch < birdwatch-dump_2023-03-09.sql`

Got some errors:

```
ERROR:  role "cloudsqladmin" does not exist
REVOKE
ERROR:  role "cloudsqlsuperuser" does not exist
GRANT
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "cloudsqlsuperuser" does not exist
ERROR:  role "rails_db_user" does not exist
ERROR:  role "rails_db_user" does not exist
ERROR:  role "rails_db_user" does not exist
ERROR:  role "rails_db_user" does not exist
```

The problems with cloudsqladmin and cloudqlsuperuser aren't surprising.

I will need to create a rails_db_user if I plan to also move the website to read from this db.

## Timing test

When I ran a simple query in the cloud, it took ~19 seconds. The same query took only ~50 milliseconds. This is going to work much better.

SQL query: `SELECT * FROM notes WHERE "tweetId" = 1337085339295506433;`

## Local db info

Host: 192.168.0.20 / localhost
Database: birdwatch
User: see .env
Password: see .env


Rails:

User: rails_db_user
Password: See Rails app secrets