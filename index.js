const Nano = require('nano')
const EventEmitter = require('events')
const url = require('url')
const qrate = require('qrate')
const cliProgress = require('cli-progress')

const isEmptyObject = function (obj) {
  return typeof obj === 'object' && Object.keys(obj).length === 0
}

// extend a URL by adding a database name
// Handles present or absent trailing slash
const extendURL = function (url, dbname) {
  if (url.match(/\/$/)) {
    return url + encodeURIComponent(dbname)
  } else {
    return url + '/' + encodeURIComponent(dbname)
  }
}

// get source document count before we start
const getStartInfo = async function (status) {
  return status.sdb.info()
}

// create the _replicator database
const createReplicator = async function (u) {
  const n = Nano(u)
  try {
    await n.db.create('_replicator')
  } catch (e) {
    // do nothing - _replicator exists already
  }
  return true
}

// start replicating by creating a _replicator document
const startReplication = async function (status, docId, sourceURL, targetURL, live) {
  // start the replication
  const obj = {
    _id: docId,
    source: sourceURL,
    target: targetURL,
    create_target: true,
    continuous: live
  }
  return status.rdb.insert(obj)
}

// fetch the replication document and the target database's info
const fetchReplicationStatusDocs = async function (status) {
  // target database
  const data = await Promise.allSettled([
    status.rdb.get(status.docId),
    status.tdb.info()
  ])
  if (data[0].status === 'fulfilled') {
    status.status = data[0].value._replication_state || 'new'
    if (typeof data[0].value._replication_stats === 'object') {
      status.docFail = data[0].value._replication_stats.doc_write_failures
    }
  }
  if (data[1].status === 'fulfilled') {
    status.targetDocCount = data[1].value.doc_count + data[1].value.doc_del_count
  }
  if (process.env.DEBUG === 'couchreplicate') {
    console.error(JSON.stringify(data))
  }
  return status
}

const wait = async function (ms) {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, ms)
  })
}

// poll the replication status until it finishes correctly or in error
const monitorReplication = async function (status, ee) {
  let finished = false

  do {
    await fetchReplicationStatusDocs(status)
    if (status.status === 'error' || status.status === 'failed') {
      status.error = true
      finished = true
    }
    if (status.status === 'completed') {
      finished = true
    }
    if (status.sourceDocCount > 0) {
      status.percent = status.targetDocCount / status.sourceDocCount
    }
    ee.emit('status', status)
    // console.log('status', status)
    if (!finished) {
      await wait(5000)
    }
  } while (!finished)
  if (status.error) {
    status.status = 'error'
    ee.emit('status', status)
    ee.emit('error', status)
  } else {
    ee.emit('completed', status)
  }
}

// migrate the _security document from the source to the target
const migrateAuth = async function (opts) {
  const securityDoc = '_security'

  // establish the source account's username
  const parsed = new url.URL(opts.sourceURL)
  let username = null
  if (parsed.auth) {
    username = parsed.auth.split(':')[0]
  }

  // fetch the source database's _security document
  const data = await opts.sdb.get(securityDoc)
  // if it's empty, do nothing
  if (isEmptyObject(data)) {
    return
  }

  // remove any reference to the source database's username
  if (username && typeof data.cloudant === 'object') {
    delete data.cloudant[username]
  }

  data._id = securityDoc
  await opts.tdb.insert(data)
}

// migrate a single database from source ---> target
const migrateSingleDB = async function (opts) {
  // sanity check URLs
  const sourceParsed = new url.URL(opts.source)
  const targetParsed = new url.URL(opts.target)

  // check source URL
  if (!sourceParsed.protocol || !sourceParsed.hostname) {
    throw new Error('invalid source URL')
  }

  // check target URL
  if (!targetParsed.protocol || !targetParsed.hostname) {
    throw new Error('invalid target URL')
  }

  // we return an event emitter so we can give real-time updates
  const ee = opts.ee || new EventEmitter()

  // extract dbname
  const rparsed = new url.URL(opts.source)

  // turn source URL into '_replicator' database
  const dbname = decodeURIComponent(sourceParsed.pathname.replace(/^\//, ''))
  rparsed.pathname = rparsed.path = '/_replicator'

  // status object
  const status = {
    replicatorURL: rparsed.href,
    sourceURL: opts.source,
    targetURL: opts.target,
    sdb: Nano(opts.source),
    tdb: Nano(opts.target),
    rdb: Nano(rparsed.href),
    dbname: dbname,
    docId: dbname.replace(/[^a-zA-Z0-9]/g, '') + '_' + (new Date()).getTime(),
    status: 'new',
    sourceDocCount: 0,
    targetDocCount: 0,
    docFail: 0,
    percent: 0,
    error: false,
    live: opts.live
  }

  const info = await getStartInfo(status)
  status.sourceDocCount = info.doc_count + info.doc_del_count

  await startReplication(status, status.docId, status.sourceURL, status.targetURL, status.live)
  ee.emit('status', status)

  // optionally migrate the auth document
  if (opts.auth) {
    await migrateAuth(status)
  }

  // monitor the replication
  if (opts.nomonitor && opts.live) {
    return status
  } else {
    try {
      await monitorReplication(status, ee)
    } catch (e) {
      let msg = 'error'
      if (e.error && e.error.reason) {
        msg += ' - ' + e.error.reason
      }
      status.status = msg
      status.error = true
      ee.emit('status', status)
      ee.emit('error', msg)
    }
  }
}

// migrate a list of documents from source --> target
const migrateList = async function (opts) {
  // enforce maximum number of continuous replications
  if (opts.live && opts.databases.length > 50) {
    throw new Error('Maximum number of continuous replications is fifty')
  }

  // ignore concurrency in live mode
  if (opts.live) {
    opts.concurrency = 50
  }

  // progress bar
  let multibar
  if (!opts.quiet) {
    // create new container
    multibar = new cliProgress.MultiBar({
      clearOnComplete: false,
      hideCursor: true,
      format: '{dbname} {bar} | {status} | ETA: {eta_formatted} | {percentage}%'
    }, cliProgress.Presets.shades_grey)
  }

  // get database names
  return new Promise((resolve, reject) => {
    // async queue of migrations
    const q = qrate(async (dbname) => {
      const newopts = JSON.parse(JSON.stringify(opts))
      if (!newopts.quiet) {
        newopts.bar = multibar.create(100, 0)
        newopts.bar.update(0, { dbname, status: '_' })
      }
      if (!opts.skipExtend) {
        newopts.source = extendURL(newopts.source, dbname)
        newopts.target = extendURL(newopts.target, dbname)
      }
      newopts.ee = new EventEmitter()
      newopts.ee.on('status', (s) => {
        if (!newopts.quiet) {
          newopts.bar.update(Math.floor(s.percent * 100), { dbname, status: s.status })
        }
      }).on('completed', (s) => {
        if (!newopts.quiet) {
          newopts.bar.update(100, { dbname, status: s.status })
        }
      })
      await migrateSingleDB(newopts)
    }, opts.concurrency)

    // push to the queue
    for (const i in opts.databases) {
      const dbname = opts.databases[i]
      if (!dbname.match(/^_/)) {
        q.push(dbname)
      }
    }

    // when the queue is drained, we're done
    q.drain = () => {
      resolve()
      if (!opts.quiet) {
        multibar.stop()
      }
    }
  })
}

// migrate all documents
const migrateAll = async function (opts) {
  // get db names and push to the queue
  const nano = Nano(opts.source)
  const data = await nano.db.list()
  opts.databases = data
  await migrateList(opts)
}

// migrate a single database
const migrateDB = async function (opts) {
  // convert to a list of database names to avoid code duplication
  const sourceParsed = new url.URL(opts.source)
  const dbname = decodeURIComponent(sourceParsed.pathname.replace(/^\//, ''))
  opts.databases = [dbname]
  opts.skipExtend = true
  await migrateList(opts)
}

module.exports = {
  migrateDB: migrateDB,
  migrateList: migrateList,
  migrateAll: migrateAll,
  createReplicator: createReplicator
}
