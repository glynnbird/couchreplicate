var cloudantqs = require('cloudant-quickstart')
var async = require('async')
var EventEmitter = require('events')
var url = require('url')

var isEmptyObject = function (obj) {
  return typeof obj === 'object' && Object.keys(obj).length === 0
}

// extend a URL by adding a database name
// Handles present or absent trailing slash
var extendURL = function (url, dbname) {
  if (url.match(/\/$/)) {
    return url + dbname
  } else {
    return url + '/' + dbname
  }
}

// get source document count before we start
var getStartInfo = function (sourceURL) {
  return cloudantqs(sourceURL).info()
}

// start replicating by creating a _replicator document
var startReplication = function (replicatorURL, docId, sourceURL, targetURL) {
  // mediator _replicator database
  var r = cloudantqs(replicatorURL)

  // start the replication
  var obj = {
    _id: docId,
    source: sourceURL,
    target: targetURL,
    create_target: true
  }
  return r.insert(obj)
}

// fetch the replication document and the target database's info
var fetchReplicationStatusDocs = function (status) {
  // mediator _replicator database
  var r = cloudantqs(status.replicatorURL)

  // target database
  var t = cloudantqs(status.targetURL)

  return Promise.all([
    r.get(status.docId),
    t.info()
  ]).then((data) => {
    status.status = data[0]._replication_state || 'new'
    status.targetDocCount = data[1].doc_count + data[1].doc_del_count
    return status
  }).catch((e) => { return status })
}

// poll the replication status until it finishes correctly or in error
var monitorReplication = function (status, ee) {
  var finished = false

  async.doUntil(
    (done) => {
       // after 5 seconds
      setTimeout(() => {
         // get the replication status
        fetchReplicationStatusDocs(status).then(() => {
          if (status.status === 'error' || status.status === 'completed') {
            finished = true
          }
          if (status.sourceDocCount > 0) {
            status.percent = status.targetDocCount / status.sourceDocCount
          }
          ee.emit('status', status)
          done()
        })
      }, 5000)
    },
    // return when finished
    () => { return finished },
    (err, results) => {
      if (err) {
        status.status = 'error'
        status.error = true
        ee.emit('error', err)
      } else {
        status.status = 'completed'
        ee.emit('status', status)
        ee.emit('completed', status)
      }
    }
  )
}

// migrate the _security document from the source to the target
var migrateAuth = function (sourceURL, targetURL) {
  return new Promise((resolve, reject) => {
    var s = cloudantqs(sourceURL)
    var t = cloudantqs(targetURL)
    var securityDoc = '_security'

    // establish the source account's username
    var parsed = url.parse(sourceURL)
    var username = null
    if (parsed.auth) {
      username = parsed.auth.split(':')[0]
    }

    // fetch the source database's _security document
    s.get(securityDoc).then((data) => {
      // if it's empty, do nothing
      if (isEmptyObject(data)) {
        return resolve()
      }

      // remove any reference to the source database's username
      if (username && typeof data.cloudant === 'object') {
        delete data.cloudant[username]
      }

      // write the source's _security document to the target
      return t.update(securityDoc, data)
    }).then((data) => {
      resolve()
    }).catch(reject)
  })
}

// migrate a single database from source ---> target
var migrateDB = function (source, target, showProgressBar, auth) {
  // we return an event emitter so we can give real-time updates
  var ee = new EventEmitter()
  var bar = null

  // extract dbname
  var parsed = url.parse(source)

  // turn source URL into '_replicator' database
  var dbname = parsed.pathname.replace(/^\//, '')
  parsed.pathname = parsed.path = '/_replicator'

  // status object
  var status = {
    replicatorURL: url.format(parsed),
    sourceURL: source,
    targetURL: target,
    dbname: dbname,
    docId: dbname + '_' + (new Date()).getTime(),
    status: 'new',
    sourceDocCount: 0,
    targetDocCount: 0,
    percent: 0,
    error: false
  }

  // initialise progress bar
  if (typeof showProgressBar === 'undefined' || showProgressBar) {
    var ProgressBar = require('ascii-progress')
    bar = new ProgressBar({
      schema: ' ' + dbname.padEnd(20) + ' [:bar.green] :percent.green :elapseds.cyan :status.blue',
      total: 100,
      status: ''
    })
  }

  return new Promise((resolve, reject) => {
    // get source doc count
    getStartInfo(status.sourceURL).then((info) => {
      status.sourceDocCount = info.doc_count + info.doc_del_count

      // start the replication
      return startReplication(status.replicatorURL, status.docId, status.sourceURL, status.targetURL)
    }).then((data) => {
      ee.emit('status', status)

      // monitor the replication
      return monitorReplication(status, ee)
    }).catch((e) => {
      var msg = 'error'
      if (e.error && e.error.reason) {
        msg += ' - ' + e.error.reason
      }
      status.status = msg
      status.error = true
      ee.emit('status', status)
      ee.emit('error', msg)
    })

    // receive status update events from the monitoring function
    ee.on('status', (s) => {
      if (bar) {
        bar.update(s.percent, { status: s.status })
      }
    })
    .on('error', (e) => {
      console.error(e)
      if (bar) {
        bar.clear()
      }
      reject(e)
    })
    .on('completed', (s) => {
      if (bar) {
        bar.update(s.percent, { status: s.status })
      }
      if (auth) {
        migrateAuth(status.sourceURL, status.targetURL).then(() => { resolve(s) })
      } else {
        resolve(s)
      }
    })
  })
}

// migrate a list of documents from source --> target
var migrateList = function (source, target, showProgressBar, dbnames, concurrency, auth) {
    // get database names
  return new Promise((resolve, reject) => {
      // async queue of migrations
    var q = async.queue((dbname, done) => {
      var sourceURL = extendURL(source, dbname)
      var targetURL = extendURL(target, dbname)
      migrateDB(sourceURL, targetURL, showProgressBar, auth).then((data) => {
        done(null, data)
      }).catch((e) => {
        done(e, null)
      })
    }, concurrency)

      // push to the queue
    for (var i in dbnames) {
      var dbname = dbnames[i]
      if (!dbname.match(/^_/)) {
        q.push(dbname)
      }
    }

      // when the queue is drained, we're done
    q.drain = () => {
      resolve()
    }
  })
}

// migrate all documents
var migrateAll = function (source, target, showProgressBar, concurrency, auth) {
  // get db names and push to the queue
  var s = cloudantqs(source, 'a')
  return s.dbs().then((data) => {
    return migrateList(source, target, showProgressBar, data, concurrency, auth)
  })
}

module.exports = {
  migrateDB: migrateDB,
  migrateList: migrateList,
  migrateAll: migrateAll
}
