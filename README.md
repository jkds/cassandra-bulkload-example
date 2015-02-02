
# cassandra-bulkload-example

Sample code which will take input from provided CSV files and export them into SSTable files
This code includes a patched Cassandra jar based on 2.0.11 containing the fix from:

[Using CQLSSTableWriter gives ConcurrentModificationException](https://issues.apache.org/jira/browse/CASSANDRA-8619)

Follows the steps below to get up and running...

----

## Creating a distribution

Create a distribution:

    $ ./gradlew distLoader

This produces a zip file under ./build/distributions called `custom-cassandra-bulkload-example.zip`

## Installing the application

Copy the zip file from the previous step to the desired location and unzip it

    $ unzip custom-cassandra-bulkload-example.zip -d {target-dir}

This produces the following structure in the target directory

    bin/
    lib/
    

## Executing the application

From the base directory run the following command:

    ./bin/cassandra-bulkload-example {input-data-directory}
    
Supplying the directory where your CSV files are stored as the argument.
Files will then be processed and output under `./data/..`


## Bulk loading

First, create schema using `schema.cql` file:

    $ cqlsh -f schema.cql

Then, load SSTables to Cassandra using `sstableloader`:

    $ sstableloader -d <ip address of the node> data/{path-to-output}

(assuming you have `cqlsh` and `sstableloader` in your `$PATH`)

## Check loaded data

#### Note:
The following is based on the original schema posted by Yuki. Yours will probably be different...


    $ bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> USE quote ;
    cqlsh:quote> SELECT * FROM historical_prices WHERE ticker = 'ORCL' LIMIT 3;

     ticker | date                     | adj_close | close | high  | low   | open  | volume
    --------+--------------------------+-----------+-------+-------+-------+-------+----------
       ORCL | 2014-09-25 00:00:00-0500 |     38.76 | 38.76 | 39.35 | 38.65 | 39.35 | 13287800
       ORCL | 2014-09-24 00:00:00-0500 |     39.42 | 39.42 | 39.56 | 38.57 | 38.77 | 18906200
       ORCL | 2014-09-23 00:00:00-0500 |     38.83 | 38.83 | 39.59 | 38.80 | 39.50 | 34353300

    (3 rows)

Voilà!
