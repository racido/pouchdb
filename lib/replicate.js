'use strict';

var utils = require('./utils');
var Pouch = require('./index');
var EE = require('events').EventEmitter;

// We create a basic promise so the caller can cancel the replication possibly
// before we have actually started listening to changes etc
utils.inherits(Replication, EE);
function Replication(opts) {
  EE.call(this);
  this.cancelled = false;
}

Replication.prototype.cancel = function () {
  this.cancelled = true;
  this.emit('cancel');
};

// A batch of changes to be processed as a unit
function Batch() {
  this.seq = 0;
  this.changes = [];
  this.docs = [];
}


// TODO: check CouchDB's replication id generation
// Generate a unique id particular to this replication
function genReplicationId(src, target, opts, callback) {
  var filterFun = opts.filter ? opts.filter.toString() : '';
  src.id(function (err, src_id) {
    target.id(function (err, target_id) {
      var queryData = src_id + target_id + filterFun +
        JSON.stringify(opts.query_params) + opts.doc_ids;
      callback('_local/' + utils.MD5(queryData));
    });
  });
}


// A checkpoint lets us restart replications from when they were last cancelled
function getCheckpoint(src, target, id) {
  return new utils.Promise(function (fulfill, reject) {
    target.get(id, function (err, targetDoc) {
      if (err && err.status === 404) {
        fulfill(0);
      } else if (err) {
        reject(err);
      } else {
        src.get(id, function (err, sourceDoc) {
          if (err && err.status === 404 ||
              (!err && (targetDoc.last_seq !== sourceDoc.last_seq))) {
            fulfill(0);
          } else if (err) {
            reject(err);
          } else {
            fulfill(sourceDoc.last_seq);
          }
        });
      }
    });
  });
}


function writeCheckpoint(src, target, id, checkpoint, callback) {
  function updateCheckpoint(db, callback) {
    db.get(id, function (err, doc) {
      if (err && err.status === 404) {
        doc = {_id: id};
      } else if (err) {
        return callback(err);
      }
      doc.last_seq = checkpoint;
      db.put(doc, callback);
    });
  }
  updateCheckpoint(target, function (err, doc) {
    if (err) { return callback(err); }
    updateCheckpoint(src, function (err, doc) {
      if (err) { return callback(err); }
      callback();
    });
  });
}


function replicate(repId, src, target, opts, returnValue) {
  var batches = [];               // list of batches to be processed
  var currentBatch;               // the batch currently being processed
  var pendingBatch = new Batch(); // next batch, not yet ready to be processed
  var writingCheckpoint = false;
  var changesCompleted = false;
  var completeCalled = false;
  var last_seq = 0;
  var continuous = opts.continuous || opts.live || false;
  var batch_size = opts.batch_size || 10;
  var batches_limit = opts.batches_limit || 10;
  var changesPending = false;
  var changeCount = 0;
  var changesPromise;
  var doc_ids = opts.doc_ids;
  var result = {
    ok: true,
    start_time: new Date(),
    docs_read: 0,
    docs_written: 0,
    doc_write_failures: 0,
    errors: []
  };
  var changesOpts = {};
  var useAllDocs = (src.type() === 'http');


  function writeDocs() {
    if (currentBatch.docs.length === 0) {
      return;
    }
    var docs = currentBatch.docs;
    return new utils.Promise(function (fulfill, reject) {
      target.bulkDocs({docs: docs}, {new_edits: false}, function (err, res) {
        if (returnValue.cancelled) {
          replicationComplete();
          return reject(new Error('cancelled'));
        }
        if (err) {
          result.doc_write_failures += docs.length;
          return reject(err);
        }
        var errors = [];
        res.forEach(function (res) {
          if (!res.ok) {
            result.doc_write_failures++;
            errors.push(new Error(res.reason || 'Unknown reason'));
          }
        });
        if (errors.length > 0) {
          var error = new Error('bulkDocs error');
          error.other_errors = errors;
          abortReplication('target.bulkDocs failed to write docs', error);
          reject(new Error('bulkWrite partial failure'));
        }
        fulfill();
      });
    });
  }


  function getNextDoc() {
    var diffs = currentBatch.diffs;
    var id = Object.keys(diffs)[0];
    var revs = diffs[id].missing;
    return src.get(id, {revs: true, open_revs: revs, attachments: true})
    .then(function (docs) {
      docs.forEach(function (doc) {
        if (returnValue.cancelled) {
          return replicationComplete();
        }
        if (doc.ok) {
          result.docs_read++;
          currentBatch.pendingRevs++;
          currentBatch.docs.push(doc.ok);
          delete diffs[doc.ok._id];
        }
      });
    });
  }


  function getAllDocs() {
    if (Object.keys(currentBatch.diffs).length > 0) {
      return getNextDoc().then(getAllDocs);
    }
  }


  function getRevisionOneDocs() {
    return new utils.Promise(function (fulfill, reject) {
      if (useAllDocs) {
        // filter out the generation 1 docs and get them
        // leaving the non-generation one docs to be got otherwise
        var ids = Object.keys(currentBatch.diffs).filter(function (id) {
          var missing = currentBatch.diffs[id].missing;
          return missing.length === 1 && missing[0].slice(0, 2) === '1-';
        });
        src.allDocs({
          keys: ids,
          include_docs: true
        }, function (err, res) {
          if (returnValue.cancelled) {
            replicationComplete();
            return reject(new Error('cancelled'));
          }
          if (err) {
            return reject(err);
          }
          res.rows.forEach(function (row, i) {
            if (row.doc && !row.deleted &&
              row.value.rev.slice(0, 2) === '1-' && (
                !row.doc._attachments ||
                Object.keys(row.doc._attachments).length === 0
              )
            ) {
              result.docs_read++;
              currentBatch.pendingRevs++;
              currentBatch.docs.push(row.doc);
              delete currentBatch.diffs[row.id];
            }
          });
          fulfill();
        });
      } else {
        fulfill();
      }
    });
  }


  function getDocs() {
    return getRevisionOneDocs()
    .then(getAllDocs);
  }


  function finishBatch() {
    return new utils.Promise(function (fulfill, reject) {
      writingCheckpoint = true;
      writeCheckpoint(
        src,
        target,
        repId,
        currentBatch.seq,
        function (err, res) {
          writingCheckpoint = false;
          if (returnValue.cancelled) {
            replicationComplete();
            reject(new Error('cancelled'));
          }
          if (err) {
            abortReplication('writeCheckpoint completed with error', err);
            reject(err);
          }
          result.last_seq = last_seq = currentBatch.seq;
          currentBatch.docs.forEach(function () {
            result.docs_written++;
            returnValue.emit('change', result);
          });
          currentBatch = undefined;
          getChanges();
          fulfill();
        }
      );
    });
  }


  function getDiffs() {
    return new utils.Promise(function (fulfill, reject) {
      var diff = {};
      currentBatch.changes.forEach(function (change) {
        diff[change.id] = change.changes.map(function (x) {
          return x.rev;
        });
      });
      target.revsDiff(diff, function (err, diffs) {
        if (returnValue.cancelled) {
          replicationComplete();
          reject(new Error('cancelled'));
        } else if (err) {
          reject(err);
        } else {
          currentBatch.diffs = diffs;
          currentBatch.pendingRevs = 0;
          fulfill();
        }
      });
    });
  }


  function startNextBatch() {
    if (
      returnValue.cancelled ||
      currentBatch
    ) {
      return;
    }
    if (batches.length === 0) {
      processPendingBatch(true);
      return;
    }
    currentBatch = batches.shift();
    getDiffs()
    .then(getDocs)
    .then(writeDocs)
    .then(finishBatch)
    .then(startNextBatch)
    .catch(function (err) {
      abortReplication('batch processing terminated with error', err);
    });
  }


  function processPendingBatch(immediate) {
    if (pendingBatch.changes.length === 0) {
      if (batches.length === 0 && !currentBatch) {
        if (continuous || changesCompleted) {
          returnValue.emit('uptodate');
        }
        if (changesCompleted) {
          replicationComplete();
        }
      }
      return;
    }
    if (
      immediate ||
      changesCompleted ||
      pendingBatch.changes.length >= batch_size
    ) {
      batches.push(pendingBatch);
      pendingBatch = new Batch();
      startNextBatch();
    }
  }


  function abortReplication(reason, err) {
    if (completeCalled) {
      return;
    }
    result.ok = false;
    result.status = 'aborted';
    err.message = reason;
    result.errors.push(err);
    batches = [];
    pendingBatch = new Batch();
    replicationComplete();
  }


  function replicationComplete() {
    if (completeCalled) {
      return;
    }
    if (returnValue.cancelled) {
      result.status = 'cancelled';
      if (writingCheckpoint) {
        return;
      }
    }
    result.status = result.status || 'complete';
    result.end_time = new Date();
    result.last_seq = last_seq;
    completeCalled = returnValue.cancelled = true;
    if (result.errors.length > 0) {
      var error = result.errors.pop();
      if (result.errors.length > 0) {
        error.other_errors = result.errors;
      }
      error.result = result;
      returnValue.emit('error', error);
    } else {
      returnValue.emit('complete', result);
    }
  }


  function onChange(change) {
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    changeCount++;
    if (changeCount > batch_size) {
      changesPromise.cancel();
      return;
    }
    if (
      pendingBatch.changes.length === 0 &&
      batches.length === 0 &&
      !currentBatch
    ) {
      returnValue.emit('outofdate');
    }
    pendingBatch.seq = change.seq;
    pendingBatch.changes.push(change);
    processPendingBatch(batches.length === 0);
  }


  function changesReject(err) {
    changesPending = false;
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    return abortReplication('changes rejected', err);
  }


  function changesFulfill(changes) {
    changesPending = false;
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    if (changes.status === 'cancelled') {
      // Workaround to leveldb limitations
      if (changeCount > 0) {
        changesOpts.since += batch_size;
        getChanges();
      } else {
        if (continuous) {
          changesOpts.live = true;
          getChanges();
        } else {
          changesCompleted = true;
        }
      }
    } else if (changes.last_seq > changesOpts.since) {
      if (changes.last_seq > changesOpts.since + batch_size) {
        changesOpts.since += batch_size;
      } else {
        changesOpts.since = changes.last_seq;
      }
      getChanges();
    } else {
      if (continuous) {
        changesOpts.live = true;
        getChanges();
      } else {
        changesCompleted = true;
      }
    }
    processPendingBatch(true);
  }

  
  function changesComplete(err, changes) {
    // Changes promise doesn't resolve when promise is cancelled
    // so use the old interface to handle this case.
    if (changes && changes.status === 'cancelled') {
      changesFulfill(changes);
    }
  }


  function getChanges() {
    if (
      !changesPending &&
      !changesCompleted &&
      batches.length < batches_limit
    ) {
      changesPending = true;
      changeCount = 0;
      changesPromise = src.changes(changesOpts);
      changesPromise.then(
        changesFulfill,
        changesReject
      );
    }
  }


  function startChanges() {
    getCheckpoint(src, target, repId).then(
    function (checkpoint) {
      last_seq = checkpoint;
      changesOpts = {
        since: last_seq,
        limit: batch_size,
        style: 'all_docs',
        doc_ids: doc_ids,
        onChange: onChange,
        // changes promise doesn't resolve when cancelled so use old complete
        complete: changesComplete,
        returnDocs: false
      };
      if (opts.filter) {
        changesOpts.filter = opts.filter;
      }
      if (opts.query_params) {
        changesOpts.query_params = opts.query_params;
      }
      getChanges();
    },
    function (err) {
      abortReplication('getCheckpoint rejected with ', err);
    });
  }


  returnValue.once('cancel', function () {
    replicationComplete();
  });

  if (typeof opts.onChange === 'function') {
    returnValue.on('change', opts.onChange);
  }

  if (typeof opts.complete === 'function') {
    returnValue.on('error', opts.complete);
    returnValue.on('complete', function (result) {
      opts.complete(null, result);
    });
  }

  if (typeof opts.since === 'undefined') {
    startChanges();
  } else {
    writeCheckpoint(src, target, repId, opts.since, function (err, res) {
      if (returnValue.cancelled) {
        return replicationComplete();
      }

      if (err) {
        return abortReplication('writeCheckpoint completed with error', err);
      }

      last_seq = opts.since;
      startChanges();
    });
  }
}


function toPouch(db) {
  if (typeof db === 'string') {
    return new Pouch(db);
  } else if (db.then) {
    return db;
  } else {
    return utils.Promise.resolve(db);
  }
}


function replicateWrapper(src, target, opts, callback) {
  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }
  if (typeof opts === 'undefined') {
    opts = {};
  }
  if (!opts.complete) {
    opts.complete = callback || function () {};
  }
  opts = utils.clone(opts);
  opts.continuous = opts.continuous || opts.live;
  var replicateRet = new Replication(opts);
  toPouch(src).then(function (src) {
    return toPouch(target).then(function (target) {
      if (opts.server) {
        if (typeof src.replicateOnServer !== 'function') {
          throw new TypeError(
            'Server replication not supported for ' + src.type() + ' adapter'
          );
        }
        if (src.type() !== target.type()) {
          throw new TypeError('Server replication' +
              ' for different adapter types (' +
            src.type() + ' and ' + target.type() + ') is not supported'
          );
        }
        src.replicateOnServer(target, opts, replicateRet);
      } else {
        genReplicationId(src, target, opts, function (repId) {
          replicate(repId, src, target, opts, replicateRet);
        });
      }
    });
  }).then(null, function (err) {
    opts.complete(err);
  });
  return replicateRet;
}

exports.replicate = replicateWrapper;
