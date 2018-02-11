var cloudantqs = require('cloudant-quickstart')
var async = require('async')
var EventEmitter = require('events');

// extend a URL by adding a database name
// Handles present or absent trailing slash
var extendURL = function(url, dbname) {
  if (url.match(/\/$/)) {
    return url + dbname
  } else {
    return url + '/' + dbname
  }
}

// get source document count before we start
var getStartInfo = function(sourceURL) {
  return cloudantqs(sourceURL).info();
}

// start replicating
var startReplication = function(replicatorURL, docId, sourceURL, targetURL) {
  
  // mediator _replicator database
  var r = cloudantqs(replicatorURL)

  // start the replication
  var obj = {
    _id: docId,
    source: sourceURL,
    target: targetURL,
    create_target: true
  }
  return r.insert(obj);
}

// fetch the replication document and the target database's info
var fetchReplicationStatusDocs = function(status) {
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
var monitorReplication = function(status, ee) {

  var finished = false

  async.doUntil(
    (done) => {
       // after 5 seconds
       setTimeout(() => {
         // get the replication status
         fetchReplicationStatusDocs(status).then(() => {
           if (status.status == 'error' || status.status == 'completed') {
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
  );

}

var migrateDB = function(source, target, dbname, showProgressBar) {

  // we return an event emitter so we can give real-time updates
  var ee = new EventEmitter();
  var bar = null;

  // status object
  var status = {
    replicatorURL: extendURL(source, '_replicator'),
    sourceURL: extendURL(source, dbname),
    targetURL: extendURL(target, dbname),
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
    var ProgressBar = require('ascii-progress');
    bar = new ProgressBar({ 
      schema: ' ' + dbname.padEnd(20) + ' [:bar.green] :percent.green :elapseds.cyan :status.blue',
      total : 100,
      status: ''
    });
  }

  return new Promise( (resolve, reject) => {
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
      var msg = 'error';
      if (e.error && e.error.reason) {
        msg += ' - ' + e.error.reason
      }
      status.status = msg
      status.error = true
      ee.emit('status', status)
      ee.emit('error', msg)
    })
    
    ee.on('status', (s) => { 
      if (bar) {
        bar.update(s.percent, { status: s.status })
      }
    })
    .on('error', (e) => { 
      console.error(e)
      if (bar) {
        bar.clear();
      }
      reject(e)
    })
    .on('completed', (s) => { 
      if (bar) {
        bar.update(s.percent, { status: s.status })
      }
      resolve(s)
    });

  });
}

var migrateList = function(source, target, showProgressBar, dbnames, concurrency) {

    // get database names
    return new Promise( (resolve, reject) => {

      // async queue of migrations
      var q = async.queue((dbname, done) => {
        migrateDB(source, target, dbname, showProgressBar).then((data) => {
          done(null, data)
        }).catch((e) => {
          done(e, null)
        })
      }, concurrency);
 
      // push to the queue
      for(var i in dbnames) {
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

var migrateAll = function(source, target, showProgressBar, concurrency) {
  // get db names and push to the queue
  var s = cloudantqs(source,'a')
  return s.dbs().then( (data) => {
    return migrateList(source, target, showProgressBar, dbnames, concurrency)
  })
}

module.exports = {
  migrateDB: migrateDB,
  migrateList: migrateList,
  migrateAll: migrateAll
}