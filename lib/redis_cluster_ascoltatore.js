"use strict";

var util = require("./util");
var wrap = util.wrap;
var TrieAscoltatore = require("./trie_ascoltatore");
var AbstractAscoltatore = require('./abstract_ascoltatore');
var SubsCounter = require("./subs_counter");
var debug = require("debug")("ascoltatori:redis_cluster");

/**
 * RedisClusterAscoltatore is a class that inherits from AbstractAscoltatore.
 * It is implemented through the `redis-node-cluster` package and it could be
 * backed up by any redis version greater than 2.2.
 *
 * The RedisClusterAscoltatore defines _rc for handling cluster conns
 *
 * The options are:
 *  - `cluster`, the redis cluster module module (it will automatically be required if not present).
 *
 * @api public
 * @param {Object} opts The options object
 */
function RedisClusterAscoltatore(opts) {
  AbstractAscoltatore.call(this, opts, {
    separator: '/',
    wildcardOne: '*',
    wildcardSome: '*'
  });

  this._opts = opts || {};
  this._redisCluster = this._opts.redisCluster || require("redis-node-cluster").Cluster;

  this._ascoltatores = {};

  this._domain = null;

  var that = this;
  this._ascoltatore = {
    registerDomain: function (domain) {
      that._domain = domain;
      for (var subTopic in that._ascoltatores) {
        that._ascoltatores[subTopic].registerDomain(domain);
      }
    }
  };

  this._startPubSub();
  
  this._subs_counter = new SubsCounter();
}



/**
 * Inheriting
 *
 * @api private
 */
RedisClusterAscoltatore.prototype = Object.create(AbstractAscoltatore.prototype);


RedisClusterAscoltatore.prototype._startPubSub = function() {
  var that = this;
  this._rc = this._opts._rc || new this._redisCluster(this._opts.nodes,this._opts.settings);
  
  this._rc.on("ready",function(){
    that.emit("ready");
  });

  //sub_handler
  var handler = function(sub, topic, message) {
    debug("new message received for topic " + topic);
    util.defer(function() {
      // we need to skip out this callback, so we do not
      // break the client when an exception occurs
      var ascoltatore = that._ascoltatores[sub];
      topic = topic.replace(new RegExp('/$'), '');
      topic = that._recvTopic(topic);

      if (ascoltatore) {
        ascoltatore.publish(topic, message);
      } else {
        debug("no ascoltatore for subscription: " + sub);
      }
    });
  };

  this._rc.on("message", function (topic, message) {
    handler(topic, topic, message);
  });
  
  this._rc.on("pmessage", function(sub, topic, message) {
    handler(sub, topic, message);
  });

  return this._rc;
};

RedisClusterAscoltatore.prototype._containsWildcard = function(topic) {
  return (topic.indexOf(this._wildcardOne) >= 0) ||
         (topic.indexOf(this._wildcardSome) >= 0);
};

RedisClusterAscoltatore.prototype.subscribe = function subscribe(topic, callback, done) {
  this._raiseIfClosed();

  var newDone = function() {
    debug("registered new subscriber for topic " + topic);
    util.defer(done);
  };

  var subTopic = this._subTopic(topic);

  if (!subTopic.match(new RegExp('[/*]$'))) {
    subTopic += '/';
  }

  if (this._containsWildcard(topic)) {
    this._rc.psubscribe(subTopic, newDone);
  } else {
    this._rc.subscribe(subTopic, newDone);
  }

  debug("redis subscription topic is " + subTopic);
  this._subs_counter.add(subTopic);

  var ascoltatore = this._ascoltatores[subTopic];

  if (!ascoltatore) {
    ascoltatore = this._ascoltatores[subTopic] = new TrieAscoltatore(this._opts);

    if (this._domain) {
      ascoltatore.registerDomain(this._domain);
    }
  }

  ascoltatore.subscribe(topic, callback);
};

RedisClusterAscoltatore.prototype.publish = function publish(topic, message, done) {
  console.log('########### PUBLISH !!!',message);
  this._raiseIfClosed();
  
  if (message === undefined || message === null) {
    message = false; // so we can convert it to JSON
  }

  topic = this._pubTopic(topic);

  if (!topic.match(new RegExp('/$'))) {
    topic += '/';
  }

  this._rc.publish(topic, message, function() {
    debug("new message published to " + topic);
    util.defer(done);
  });
};

RedisClusterAscoltatore.prototype.unsubscribe = function unsubscribe(topic, callback, done) {
  this._raiseIfClosed();

  var isWildcard = this._containsWildcard(topic),
      subTopic = this._subTopic(topic);

  if (!subTopic.match(new RegExp('[/*]$'))) {
    subTopic += '/';
  }

  this._subs_counter.remove(subTopic);

  var ascoltatore = this._ascoltatores[subTopic];

  if (ascoltatore) {
    ascoltatore.unsubscribe(topic, callback);
  }

  var newDone = function() {
    debug("deregistered subscriber for topic " + topic);
    util.defer(done);
  };

  if (this._subs_counter.include(subTopic)) {
    newDone();
    return this;
  }

  if (ascoltatore) {
    ascoltatore.close();
    delete this._ascoltatores[subTopic];
  }

  if (isWildcard) {
    this._rc.punsubscribe(subTopic, newDone);
  } else {
    this._rc.unsubscribe(subTopic, newDone);
  }

  return this;
};

RedisClusterAscoltatore.prototype.close = function close(done) {
  var that = this,
    newDone = null;

  newDone = function() {
    debug("closed");
    util.defer(done);
  };

  if (this._closed) {
    newDone();
    return;
  }

  this._subs_counter.clear();
  
  this._rc.disconnect(newDone);

  for (var subTopic in this._ascoltatores) {
    this._ascoltatores[subTopic].close();
  }
  this._ascoltatores = {};

  this.emit("closed");
};

util.aliasAscoltatore(RedisClusterAscoltatore.prototype);

/**
 * Exports the RedisClusterAscoltatore
 *
 * @api public
 */
module.exports = RedisClusterAscoltatore;
