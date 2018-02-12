# couchreplicate

[![Build Status](https://travis-ci.org/ibm-watson-data-lab/couchreplicate.svg?branch=master)](https://travis-ci.org/ibm-watson-data-lab/couchreplicate) [![npm version](https://badge.fury.io/js/couchreplicate.svg)](https://badge.fury.io/js/couchreplicate)

This is a command-line tool and library that helps coordinate [Apache CouchDB](http://couchdb.apache.org/)™ or [IBM Cloudant](https://www.ibm.com/cloud/cloudant) replications. It can be used to replicate a single database, multiple databases or an entire cluster from a source instance to a target instance.

It is written in Node.js and can be installed from `npm` or used programmatically in your own Node.js projects.




## Pre-requisites

- [Node.js & npm](https://nodejs.org/en/)
- one or more source databases in an Apache CouchDB or IBM Cloudant instance
- a target Apache CouchDB or IBM Cloudant instance

## Installation

Install with npm

    npm install -g couchreplicate

## Usage

### Replicating all databases from one instance to another

    couchreplicate -a -s http://u:p@localhost:5984 -t https://U:P@HOST.cloudant.com

or 

    couchreplicate --all --source http://u:p@localhost:5984 --target https://U:P@HOST.cloudant.com

where `http://u:p@localhost:5984` is the URL of your source instance and `https://U:P@HOST.cloudant.com` is the URL of your target instance. They must include `admin` credentials.

### Replicating a list of databases

    couchreplicate -d db1,db2,db3 -s http://u:p@localhost:5984 -t https://U:P@HOST.cloudant.com

where `db1,db2,db3` is a comma-separated list of database names.

### Replicating a single database

    couchreplicate -d mydb -s http://u:p@localhost:5984 -t https://U:P@HOST.cloudant.com

or 

    couchreplicate -s http://u:p@localhost:5984/mydb -t https://U:P@HOST.cloudant.com/mydb

where `mydb` is the name of the database. When supplying the source and target database names in the URL, the database names need not match.

## Command-line parameters

- `--source` / `-s` - source URL (required)
- `--target` / `-t` - target URL (required)
- `--databases` / `-d` - comma-separated list of database names
- `--all` / `-a` - replicate all databases
- `--concurrency` / `-c` - the number of replications to run at once (default = 1)
- `--quiet` / `-q` - suppress progress bars
- `--help` / `-h` - show help message
- `--version` / `-v` - show version number

## Using couchreplicate programmatically

Install the library into your own Node.js project

    npm install --save couchreplicate

Load the module into your code

```js
  const cm = require('couchmigrate')
```

Set off a single replication:

```js
  const srcURL = "http://u:p@localhost:5984/sourcedb"
  const targetURL = "https://U:P@HOST.cloudant.com/targetdb"
  const showProgressBar = false
  
  cm.migrateDB(srcURL, targetURL, showProgressBar).then(() => {
    console.log('done')
  })
```

multiple replications:

```js
  const srcURL = "http://u:p@localhost:5984"
  const targetURL = "https://U:P@HOST.cloudant.com"
  const showProgressBar = false
  const databases = ['animals', 'minerals', 'vegetables']
  const concurrency = 3
  
  cm.migrateList(srcURL, targetURL, showProgressBar, databases, concurrency).then(() => {
    console.log('done')
  })
```

or an entire cluster:

```js
  const srcURL = "http://u:p@localhost:5984"
  const targetURL = "https://U:P@HOST.cloudant.com"
  const showProgressBar = false
  const databases = ['animals', 'minerals', 'vegetables']
  const concurrency = 3
  
  cam.migrateAll(srcURL, targetURL, showProgressBar, concurrency).then(() =>
=> {
    console.log('done')
  })
```