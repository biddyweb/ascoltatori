"use strict";

var util = require("./util");
var wrap = util.wrap;
var TrieAscoltatore = require("./trie_ascoltatore");
var AbstractAscoltatore = require('./abstract_ascoltatore');
var async = require("async");
var SubsCounter = require("./subs_counter");
var debug = require("debug")("ascoltatori:amqp");

/**
 * The AMQPAscoltatore is a class that inherits from AbstractAscoltatore.
 * It is backed by node-amqp.
 * It creates or use an exchange with the given name, using a "topic" topology.
 * It creates a single amqp queue for this process, in order to keep
 * the overhead low.
 *
 * It accepts these options:
 *  - `client`, which is passed through to the amq.createConnection method;
 *  - `exchange`, the exchange name;
 *  - `amqp`, the amqp module (it will automatically be required if not present);
 *
 * @param {Object} opts The options for creating this ascoltatore.
 * @api public
 */

function AMQPAscoltatore(opts) {
  AbstractAscoltatore.call(this, opts, {
    separator: '.',
    wildcardOne: '*',
    wildcardSome: '#'
  });

  this._opts = opts || {};
  this._opts.amqp = this._opts.amqp || require("amqp");
  this._ascoltatore = new TrieAscoltatore(opts);

  this._subs_counter = new SubsCounter();
  this._startConn();
  this.dataV1Queue  = "mqtt-broker:device:data";
  this.powerQueue = "mqtt-broker:device:power";
  this.dataV2Queue  = "mqtt-broker:device:v2:data";
  this.deviceConnectQueue  = "mqtt-broker:device:v2:connect";
  this.transmitterConnectQueue  = "mqtt-broker:transmitter:v2:connect";
}

/**
 * The client connection decends from AbstractAscoltatore.
 *
 * @api private
 */
AMQPAscoltatore.prototype = Object.create(AbstractAscoltatore.prototype);

/**
 * Starts a new connection to an AMQP server.
 * Do nothing if it is already started.
 *
 * @api private
 */
AMQPAscoltatore.prototype._startConn = function() {
  var conn = null,
    that = this;

  if (this._client_conn === undefined) {

    conn = this._opts.amqp.createConnection(this._opts.client);
    this._client_conn = conn;

    conn.on("error", function(error) {
      if (typeof error === 'string') {
        error = (new Error(error));
      }

      that.emit("error", error);
    });

    debug("connecting to " + this._opts.client);

    async.series([

      function(callback) {
        that._client_conn.once("ready", wrap(callback));
      },

      function(callback) {
        debug("connected");
        that._exchange = conn.exchange(that._opts.exchange, {
          type: "topic",
          confirm: true
        });
        that._exchange.once("open", wrap(callback));
      },

      function(callback) {
        debug("created exchange " + that._opts.exchange);
        that._queue = conn.queue(util.buildIdentifier(), { exclusive : true }, wrap(callback));
        that._queue.setMaxListeners(0); // avoid problems with listeners
      },

      function(callback) {
        that._queue.subscribe({
          ack: true,
          prefetchCount: 42
        }, function(message, headers, deliveryInfo) {
          that._queue.shift();

          var topic = that._recvTopic(deliveryInfo.routingKey);

          debug("new message received from queue on topic " + topic);

          that._ascoltatore.publish(topic, message.data.toString());
        });
        that._queue.once("basicConsumeOk", function() {
          util.defer(callback);
        });
      },

      function(callback) {
        debug("subscribed to queue");
        that.emit("ready");
        callback();
      }
    ]);
  }
  return this._client_conn;
};

AMQPAscoltatore.prototype.subscribe = function subscribe(topic, callback, done) {
  this._raiseIfClosed();

  this._ascoltatore.subscribe(topic, callback);

  if (!this._subs_counter.include(topic)) {

    debug("binding queue to topic " + topic);

    this._queue.once("queueBindOk", function() {
      // trick against node-amqp not working
      // as advertised
      setTimeout(function() {
        debug("queue bound to topic " + topic);
        util.defer(done);
      }, 5);
    });

    this._queue.bind(this._exchange, this._subTopic(topic));
  } else {
    util.defer(done);
  }

  this._subs_counter.add(topic);

  debug("registered new subscriber for topic " + topic);
};

var oneMinute = 1 * 60 * 1000 + "";

function endsWith(str, suffix) {
  return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

AMQPAscoltatore.prototype.publish = function publish(topic, message, done) {
  var self = this;
  this._raiseIfClosed();

  debug("new message published to " + topic);

  var topicParts = topic.split("/");

  var amqpForwardMessage = function(queue, headers) {
    self._client_conn.publish(queue, message, { headers: headers, expiration: oneMinute });
  };

  var version = topicParts[1];
  if(version === "v1") {
    var deviceId = topicParts[2];
    if(endsWith(topic, "/data")){
      amqpForwardMessage(this.dataV1Queue, { deviceId: deviceId, mqttTopic: topic });
    }
    else if(endsWith(topic, "/data/power")){
      amqpForwardMessage(this.powerQueue, { deviceId: deviceId, mqttTopic: topic });
    }
  } else if(version === "v2") {
    var collection = topicParts[3];
    var itemId = topicParts[2];
    var eventClass = topicParts[4];
    if(collection === "transmitter" && eventClass === "presence" && topicParts[5] === "connect"){
      amqpForwardMessage(this.transmitterConnectQueue, { transmitterId: itemId, mqttTopic: topic });
    } else if(collection === "device" && eventClass === "presence" && topicParts[5] === "connect"){
      amqpForwardMessage(this.deviceConnectQueue, { deviceId: itemId, mqttTopic: topic });
    } else if(collection === "device" && eventClass === "data"){
      amqpForwardMessage(this.dataV2Queue, { deviceId: itemId, mqttTopic: topic });
    }
  }

  this._exchange.publish(this._pubTopic(topic), String(message), { expiration: oneMinute });
  util.defer(done);
};

AMQPAscoltatore.prototype.unsubscribe = function unsubscribe(topic, callback, done) {
  this._raiseIfClosed();
  this._subs_counter.remove(topic);

  debug("deregistered subscriber for topic " + topic);

  this._ascoltatore.unsubscribe(topic, callback);

  if (!this._subs_counter.include(topic)) {
    this._queue.once("queueUnbindOk", function() {
      debug("queue unbound to topic " + topic);
      util.defer(done);
    });

    this._queue.unbind(this._exchange, this._subTopic(topic));
  } else {
    util.defer(done);
  }

  return this;
};

AMQPAscoltatore.prototype.close = function close(done) {
  var that = this;

  if (this._closed) {
    wrap(done)();
    return;
  }

  if (this._closing) {
    this.on("closed", done);
    return;
  }

  this._closing = true;

  if (this._client_conn !== undefined) {
    var doClose = function () {
      if (that._closed) {
        debug("closing twice, one was an error");
        return;
      }

      debug("closed");
      util.defer(done);
      that.emit("closed");
    };

    this._client_conn.on("close", doClose);

    this._queue.destroy();
    this._client_conn.end();
    this._client_conn.removeAllListeners("error");
    this._client_conn.on("error", doClose);

    delete this._client_conn;
    delete this._exchange;
    delete this._queue;
  }
};

util.aliasAscoltatore(AMQPAscoltatore.prototype);

/**
 * Exports the AMQPAscoltatore
 *
 * @api public
 */
module.exports = AMQPAscoltatore;
