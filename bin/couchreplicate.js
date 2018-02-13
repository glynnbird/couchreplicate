#!/usr/bin/env node

var cam = require('../index.js')
var url = require('url')

var argv = require('yargs')
  .option('source', { alias: 's', describe: 'Cloudant source URL', demandOption: true })
  .option('target', { alias: 't', describe: 'Cloudant target URL', demandOption: true })
  .option('concurrency', { alias: 'c', describe: 'Number of replications to run at once', demandOption: false, default: 1 })
  .option('databases', { alias: 'd', describe: 'Names of the database to replicate e.g. a,b,c', demandOption: false, default: '' })
  .option('all', { alias: 'a', describe: 'Replicate all databases', demandOption: false, default: false })
  .option('auth', { alias: 'x', describe: 'Also copy _security document', demandOption: false, default: false })
  .option('quiet', { alias: 'q', describe: 'Supress progress bars', demandOption: false, default: false })
  .help('help')
  .argv

var sourceParsed = url.parse(argv.source)
var targetParsed = url.parse(argv.target)

// check source URL
if (!sourceParsed.protocol || !sourceParsed.hostname) {
  console.error('invalid source URL')
  process.exit(1)
}

// check target URL
if (!targetParsed.protocol || !targetParsed.hostname) {
  console.error('invalid target URL')
  process.exit(1)
}

// ensure that if database names are supplied in the URLs that
// there is both a source and target name
var sourceDbname = sourceParsed.pathname.replace(/^\//, '')
var targetDbname = sourceParsed.pathname.replace(/^\//, '')

// not databases names supplied anywhere
if (!sourceDbname && !targetDbname && !argv.databases && !argv.all) {
  console.error('ERROR: no source or target database names supplied.')
  console.error('Either:')
  console.error(' 1) supply source and target database names in the URLs')
  console.error(' 2) supply database name(s) with -d or --databases parameters')
  console.error(' 3) use the -a parameter to replicate all databases')
  process.exit(2)
}

// database names supplied in URLs and in other parameters
if ((sourceDbname || targetDbname) && (argv.databases || argv.all)) {
  console.error('ERROR: database names supplied in URLs and as other command-line options')
  process.exit(3)
}

// if URLS contain database names
if (sourceDbname && targetDbname) {
  // migrate single database
  cam.migrateDB(argv).then(() => {})
} else if (argv.databases) {
  // or if a named database or list is supplied
  argv.databases = argv.databases.split(',')
  cam.migrateList(argv).then(() => {})
} else if (argv.all) {
  // or if all databases are required
  cam.migrateAll(argv).then(() => {})
}
