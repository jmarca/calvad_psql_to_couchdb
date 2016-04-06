# Copy psql vds metadata to vdsdata%2ftracking in couchdb

This repo holds code that keeps couchdb vds metadata in sync with my psql
metadata.

Actually, as I update this in April 2016, I'm realizing that the
choices I made back in 2002ish for storing vds metadata from Caltrans
sources were not super smart.  It appears that I didn't allow for lots
of things that change with changing versions...postmiles, geometries,
etc.

This code just institutionalizes that into couchdb.  Worse than that,
it will take the latest value for certain items and store them as if
they were that way forever (detector type, milepost, etc)

What I really need to do is mimic the metadata parsing code, but for
couchdb.  Because it isn't a relational db, it would be possible to
store the metadata in a proper versioned approach.

# Options and other notes

The original copy from sql to couch was done with perl.  When I first
ran *this* code to do that job back in 2014, I just used the test
function `./test/test_write_to_couchdb.js`.  Today I updated that test
to be a real test, and instead created the executable
`./update_couchdb.js`.

To run it, set the appropriate parameters in config.json.

For example:

```javascript
{
    "couchdb": {
        "host": "127.0.0.1",
        "port":5984,
        "auth":{"username":"james",
                "password":"My what big eyes you have"
               },
        "db":"vdsdata%2ftracking"
    },
    "postgresql":{
        "host":"127.0.0.1",
        "port":5432,
        "auth":{"username":"james"},
        "db":"spatialvds"
    },
    "id":1009910,
    "years":2014
}
```

You only need the password in the postgresql settings if you aren't
using a .pgpass file, or perhaps if it is broken for some reason.  Try
it without first.

My config reader program,
[`config_okay`](https://github.com/jmarca/config_okay) requires that
the configuration file be set with restrictive permissions.  So do
`chmod 0600 config.json` in order to satisfy these requirements.

Other options are to run the program for a single vds id (above the
option is set to run for just 1009910), and to run
it for only recent years (the setting above will query and copy only
records from the year 2014 and greater (2015, 2016, etc)).  There is
also an option to set specific years.  Pass an array if that is the
case.  So to just update 2014, you would do:

```
    "years":[2014]
```

# Testing

Testing uses a config file called `test.config.json`.  Which is
exactly like the above config.json, but without the years and id
options (those get set in the tests as needed).

A fake couchdb database will be created and destroyed for each test, as
needed.

Test it with

```
npm install
./node_modules/.bin/mocha --timeout 5000
```

The important tests will pass, but `./test/test_query_vds_info.js`
will fail at the moment because I'm too lazy to fix it right now...it
uses an older way of calling node-postgresql and couchdb, etc.


# running it

Run the program with

```
npm install
node update_couchdb.js > update.out 2>&1 &
```


Go have a coffee.  When you get back, it should be done.
