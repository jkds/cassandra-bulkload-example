CREATE KEYSPACE IF NOT EXISTS staging WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE staging;
create table if not EXISTS staging.temp_profilereads(
id1 text,
id2 text,
type text,
units text,
date text,
year text,
month text,
day text,
time text,
sample text,
primary key ((year, month, day), id2, time)
);
