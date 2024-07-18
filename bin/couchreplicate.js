#!/usr/bin/env node

const cam = require('../index.js')
const url = require('url')
const syntax =
`Syntax:
--source/-s                   CouchDB source URL                              (required)
--target/-t                   CouchDB target URL                              (required)
--concurrency/-c              Number of replications to run at once           (default: 1)
--databases/-d                Names of databases to replicate e.g. a,b,c
--all/-a                      Replicate all databases
--auth/-x                     Also copy _security document
--quiet/-q                    Supress progress bars                          (default: false)
--live/-l                     Setup live (continuous) replications instead   (default: false)
--nomonitor/-n                Don't monitor the replications after setup     (default: false)
--deletions                         Include deleted docs (default: false)
`
const { parseArgs } = require('node:util')
const argv = process.argv.slice(2)
const options = {
  source: {
    type: 'string',
    short: 's'
  },
  target: {
    type: 'string',
    short: 't'
  },
  concurrency: {
    type: 'string',
    short: 'c',
    default: '1'
  },
  databases: {
    type: 'string',
    short: 'd'
  },
  all: {
    type: 'boolean',
    short: 'a',
    default: false
  },
  auth: {
    type: 'boolean',
    short: 'x',
    default: false
  },
  quiet: {
    type: 'boolean',
    short: 'q',
    default: false
  },
  nomonitor: {
    type: 'boolean',
    short: 'n',
    default: false
  },
  help: {
    type: 'boolean',
    short: 'h',
    default: false
  }
}

// parse command-line options
const { values } = parseArgs({ argv, options })

// help mode
if (values.help) {
  console.log(syntax)
  process.exit(0)
}

// string to int
values.concurrency = parseInt(values.concurrency)

// parse the URLs
const sourceParsed = new url.URL(values.source)
const targetParsed = new url.URL(values.target)

// check source URL
if (!sourceParsed.protocol || !sourceParsed.hostname) {
  console.error('Error: invalid source URL')
  process.exit(1)
}

// check target URL
if (!targetParsed.protocol || !targetParsed.hostname) {
  console.error('Error: invalid target URL')
  process.exit(2)
}

// check for --nomonitor without live mode
if (values.nomonitor && !values.live) {
  console.error('Error: --nomonitor/-n is only applicable with the --live/-l option')
  process.exit(3)
}

// ensure that if database names are supplied in the URLs that
// there is both a source and target name
const sourceDbname = sourceParsed.pathname.replace(/^\//, '')
const targetDbname = sourceParsed.pathname.replace(/^\//, '')

// not databases names supplied anywhere
if (!sourceDbname && !targetDbname && !values.databases && !values.all) {
  console.error('ERROR: no source or target database names supplied.')
  console.error('Either:')
  console.error(' 1) supply source and target database names in the URLs')
  console.error(' 2) supply database name(s) with -d or --databases parameters')
  console.error(' 3) use the -a parameter to replicate all databases')
  process.exit(4)
}

// database names supplied in URLs and in other parameters
if ((sourceDbname || targetDbname) && (values.databases || values.all)) {
  console.error('ERROR: database names supplied in URLs and as other command-line options')
  process.exit(5)
}

// calculate the replicatorURL
sourceParsed.pathname = sourceParsed.path = ''
const replicatorURL = sourceParsed.href

const main = async () => {
  // if URLS contain database names
  if (sourceDbname && targetDbname) {
    // migrate single database
    try {
      await cam.createReplicator(replicatorURL)
      await cam.migrateDB(values)
    } catch (e) {
      console.error(e)
      process.exit(6)
    }
  } else if (values.databases) {
    // or if a named database or list is supplied
    values.databases = values.databases.split(',')
    try {
      await cam.createReplicator(replicatorURL)
      await cam.migrateList(values)
    } catch (e) {
      console.error(e)
      process.exit(6)
    }
  } else if (values.all) {
    // or if all databases are required
    try {
      await cam.createReplicator(replicatorURL)
      await cam.migrateAll(values)
    } catch (e) {
      console.error(e)
      process.exit(6)
    }
  }
}

main()
