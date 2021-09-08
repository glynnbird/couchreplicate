#!/usr/bin/env node

const cam = require('../index.js')
const url = require('url')

const argv = require('yargs')
  .option('source', { alias: 's', describe: 'Cloudant source URL', demandOption: true })
  .option('target', { alias: 't', describe: 'Cloudant target URL', demandOption: true })
  .option('concurrency', { alias: 'c', describe: 'Number of replications to run at once', demandOption: false, default: 1 })
  .option('databases', { alias: 'd', describe: 'Names of the database to replicate e.g. a,b,c', demandOption: false, default: '' })
  .option('all', { alias: 'a', describe: 'Replicate all databases', demandOption: false, default: false })
  .option('auth', { alias: 'x', describe: 'Also copy _security document', demandOption: false, default: false })
  .option('quiet', { alias: 'q', describe: 'Supress progress bars', demandOption: false, default: false })
  .option('live', { alias: 'l', describe: 'Setup live (continuous) replications instead', demandOption: false, default: false })
  .option('nomonitor', { alias: 'n', describe: 'Don\'t monitor the replications after setup', demandOption: false, default: false })
  .help('help')
  .argv

const sourceParsed = new url.URL(argv.source)
const targetParsed = new url.URL(argv.target)

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
if (argv.nomonitor && !argv.live) {
  console.error('Error: --nomonitor/-n is only applicable with the --live/-l option')
  process.exit(3)
}

// ensure that if database names are supplied in the URLs that
// there is both a source and target name
const sourceDbname = sourceParsed.pathname.replace(/^\//, '')
const targetDbname = sourceParsed.pathname.replace(/^\//, '')

// not databases names supplied anywhere
if (!sourceDbname && !targetDbname && !argv.databases && !argv.all) {
  console.error('ERROR: no source or target database names supplied.')
  console.error('Either:')
  console.error(' 1) supply source and target database names in the URLs')
  console.error(' 2) supply database name(s) with -d or --databases parameters')
  console.error(' 3) use the -a parameter to replicate all databases')
  process.exit(4)
}

// database names supplied in URLs and in other parameters
if ((sourceDbname || targetDbname) && (argv.databases || argv.all)) {
  console.error('ERROR: database names supplied in URLs and as other command-line options')
  process.exit(5)
}

// calculate the replicatorURL
sourceParsed.pathname = sourceParsed.path = ''
const replicatorURL = sourceParsed.href

// if URLS contain database names
if (sourceDbname && targetDbname) {
  // migrate single database
  cam.createReplicator(replicatorURL).then(() => {
    return cam.migrateDB(argv)
  }).then(() => {}).catch((e) => {
    console.error(e)
    process.exit(6)
  })
} else if (argv.databases) {
  // or if a named database or list is supplied
  argv.databases = argv.databases.split(',')
  cam.createReplicator(replicatorURL).then(() => {
    return cam.migrateList(argv)
  }).then(() => {}).catch((e) => {
    console.error(e)
    process.exit(6)
  })
} else if (argv.all) {
  // or if all databases are required
  cam.createReplicator(replicatorURL).then(() => {
    return cam.migrateAll(argv)
  }).then(() => {}).catch((e) => {
    console.error(e)
    process.exit(6)
  })
}
