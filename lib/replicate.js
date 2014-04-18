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
function fetchCheckpoint(src, target, id, callback) {
  target.get(id, function (err, targetDoc) {
    if (err && err.status === 404) {
      callback(null, 0);
    } else if (err) {
      callback(err);
    } else {
      src.get(id, function (err, sourceDoc) {
        if (err && err.status === 404 ||
            (!err && (targetDoc.last_seq !== sourceDoc.last_seq))) {
          callback(null, 0);
        } else if (err) {
          callback(err);
        } else {
          callback(null, sourceDoc.last_seq);
        }
      });
    }
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
  var fetchAgain = [];  // queue of documents to be fetched again with api.get
  var writingCheckpoint = false;
  var changesCompleted = false;
  var completeCalled = false;
  var last_seq = 0;
  var continuous = opts.continuous || opts.live || false;
  var batch_size = opts.batch_size || 100;
  var batches_limit = opts.batches_limit || 100;
  var changesPending = false;
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


  function writeDocs() {
    if (currentBatch.docs.length === 0) {
      return finishBatch();
    }
    var docs = currentBatch.docs;
    target.bulkDocs({docs: docs}, {new_edits: false}, function (err, res) {
      if (returnValue.cancelled) {
        return replicationComplete();
      }
      if (err) {
        console.log('bulk docs error ' + JSON.stringify(err));
        result.doc_write_failures += docs.length;
        return abortReplication('target.bulkDocs completed with error', err);
      }
      var errors = [];
      res.forEach(function (res) {
        if (!res.ok) {
          result.doc_write_failures++;
          console.log('document write error ' + JSON.stringify(res));
          errors.push(new Error(res.reason || 'Unknown reason'));
        }
      });
      if (errors.length > 0) {
        var error = new Error('bulkDocs error');
        error.other_errors = errors;
        return abortReplication('target.bulkDocs failed to write docs', error);
      }
      finishBatch();
    });
  }


  function onGetError(err) {
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    return abortReplication('src.get completed with error', err);
  }


  function onGet(docs) {
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    Object.keys(docs).forEach(function (revpos) {
      var doc = docs[revpos].ok;
      if (doc) {
        result.docs_read++;
        currentBatch.pendingRevs++;
        currentBatch.docs.push(doc);
      }
    });
    fetchRev();
  }


  function fetchGenerationOneRevs(ids, revs) {
    src.allDocs({
      keys: ids,
      include_docs: true
    }, function (err, res) {
      if (returnValue.cancelled) {
        return replicationComplete();
      }
      if (err) {
        return abortReplication('src.get completed with error', err);
      }
      res.rows.forEach(function (row, i) {
        // fetch document again via api.get when doc
        // * is deleted document (could have data)
        // * is no longer generation 1
        // * has attachments
        var needsSingleFetch = !row.doc ||
          row.value.rev.slice(0, 2) !== '1-' ||
          row.doc._attachments && Object.keys(row.doc._attachments).length;

        if (needsSingleFetch) {
          return fetchAgain.push({
            id: row.error === 'not_found' ? row.key : row.id,
            rev: revs[i]
          });
        }
        result.docs_read++;
        currentBatch.pendingRevs++;
        currentBatch.docs.push(row.doc);
      });
      fetchRev();
    });
  }

  
  function fetchRev() {
    if (fetchAgain.length) {
      var doc = fetchAgain.shift();
      return fetchSingleRev(src, doc.id, [doc.rev]).then(onGet, onGetError);
    }
    var diffs = currentBatch.diffs;
    if (Object.keys(diffs).length === 0) {
      writeDocs();
      return;
    }
    var generationOne = Object.keys(diffs).reduce(function (memo, id) {
      if (diffs[id].missing.length === 1 &&
          diffs[id].missing[0].slice(0, 2) === '1-') {
        memo.ids.push(id);
        memo.revs.push(diffs[id].missing[0]);
        delete diffs[id];
      }
      return memo;
    }, {
      ids: [],
      revs: []
    });
    if (generationOne.ids.length) {
      return fetchGenerationOneRevs(generationOne.ids, generationOne.revs);
    }
    var id = Object.keys(diffs)[0];
    var revs = diffs[id].missing;
    delete diffs[id];
    fetchSingleRev(src, id, revs).then(onGet, onGetError);
  }


  function finishBatch() {
    writingCheckpoint = true;
    writeCheckpoint(src, target, repId, currentBatch.seq, function (err, res) {
      writingCheckpoint = false;
      if (returnValue.cancelled) {
        return replicationComplete();
      }
      if (err) {
        return abortReplication('writeCheckpoint completed with error', err);
      }
      result.last_seq = last_seq = currentBatch.seq;
      currentBatch.docs.forEach(function () {
        result.docs_written++;
        returnValue.emit('change', result);
      });
      currentBatch = undefined;
      startNextBatch();
      getChanges();
    });
  }


  function onRevsDiff(err, diffs) {
    if (returnValue.cancelled) {
      return replicationComplete();
    }
    if (err) {
      return abortReplication('target.revsDiff completed with error', err);
    }
    if (Object.keys(diffs).length === 0) {
      finishBatch();
      return;
    }
    currentBatch.diffs = diffs;
    currentBatch.pendingRevs = 0;
    fetchRev();
  }


  function startNextBatch() {
    if (currentBatch) {
      return;
    }
    if (batches.length === 0) {
      processPendingBatch(true);
      return;
    }
    currentBatch = batches.shift();
    var diff = {};
    currentBatch.changes.forEach(function (change) {
      diff[change.id] = change.changes.map(function (x) {
        return x.rev;
      });
    });
    target.revsDiff(diff, onRevsDiff);
  }


  function processPendingBatch(immediate) {
    if (pendingBatch.changes.length === 0) {
      if (batches.length === 0 && !currentBatch) {
        if (changesOpts.live || changesCompleted) {
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
    console.log('abortReplication ' + JSON.stringify(arguments));
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
      console.log('completing with error(s)' + JSON.stringify(result.errors));
      var error = result.errors.pop();
      console.log('error ' + JSON.stringify(error));
      if (result.errors.length > 0) {
        error.other_errors = result.errors;
      }
      error.result = result;
      return opts.complete(error);
    } else {
      return opts.complete(null, result);
    }
  }


  function onChange(change) {
    if (returnValue.cancelled) {
      return replicationComplete();
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
    if (changes.last_seq > changesOpts.since) {
      changesOpts.since = changes.last_seq;
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


  function getChanges() {
    if (
      !changesPending &&
      !changesCompleted &&
      batches.length < batches_limit
    ) {
      changesPending = true;
      src.changes(changesOpts).then(changesFulfill, changesReject);
    }
  }


  function startChanges() {
    fetchCheckpoint(src, target, repId, function (err, checkpoint) {
      if (returnValue.cancelled) {
        return replicationComplete();
      }

      if (err) {
        return abortReplication('fetchCheckpoint completed with error', err);
      }

      last_seq = checkpoint;

      changesOpts = {
        since: last_seq,
        limit: batch_size,
        style: 'all_docs',
        doc_ids: doc_ids,
        onChange: onChange,
        returnDocs: false
      };

      if (opts.filter) {
        changesOpts.filter = opts.filter;
      }

      if (opts.query_params) {
        changesOpts.query_params = opts.query_params;
      }

      getChanges();
    });
  }


  returnValue.once('cancel', function () {
    replicationComplete();
  });

  returnValue.on('change', function (result) {
    utils.call(opts.onChange, result);
  });


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


function fetchSingleRev(src, id, revs) {
  return src.get(id, {revs: true, open_revs: revs, attachments: true});
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
