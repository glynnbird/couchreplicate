#!/usr/bin/env node

var cam = require('../index.js')

var argv = require('yargs')
  .option('source', { alias: 's',describe:'Cloudant source URL', demandOption: true })
  .option('target', { alias: 't',describe:'Cloudant target URL', demandOption: true })
  .option('concurrency', { alias: 'c',describe:'Number of replications to run at once', demandOption: false, default: 1})
  .option('databases', { alias: 'd',describe:'Names of the database to replicate e.g. a,b,c', demandOption: false, default: ''})
  .option('all', { alias: 'd',describe:'Names of the database to replicate e.g. a,b,c', demandOption: false, default: ''})
  .option('quiet', { alias: 'q',describe:'Supress progress bars', demandOption: false, default: false})
  .help('help')
  .argv

var progressBar = !argv.quiet
if (argv.databases) {
  var databases = argv.databases.split(',')
  console.log('Replicating', databases)
  cam.migrateList(argv.source, argv.target, progressBar, databases, argv.concurrency).then(() => {})
} else {
  console.log('Replicating all databases')
  cam.migrateAll(argv.source, argv.target, progressBar, argv.concurrency).then(() => {})
}
