var fs = require("fs");

describeAscoltatore("redis", function() {

  afterEach(function() {
    this.instance.close();
  });

  it("should publish into redis a binary payload", function(done) {
    this.instance.close();

    var settings = redisSettings();
    settings.return_buffers = true;

    this.instance = new ascoltatori.RedisAscoltatore(settings);

    var that = this;
    var expected = fs.readFileSync(__dirname + "/image.png");
    console.log('expected',expected);
    that.instance.sub("image", function(topic, value) {
      expect(value).to.eql(expected);
      done();
    }, function() {
      that.instance.pub("image", expected);
    });
  });

  it("should sync two redis instances", function(done) {
    var other = new ascoltatori.RedisAscoltatore(redisSettings());
    var that = this;
    async.series([

      function(cb) {
        other.on("ready", cb);
      },

      function(cb) {
        that.instance.subscribe("hello", wrap(done), cb);
      },

      function(cb) {
        other.publish("hello", null, cb);
      }
    ]);
  });

  it('should get the redis client for publish already created', function(done) {
    var opts = redisSettings();
    var initialConnection = opts.redis.createClient(opts.port, opts.host, opts);
    opts.client_conn = initialConnection;
    var that = this;
    that.instance = new ascoltatori.RedisAscoltatore(opts);
    that.instance.subscribe("hello", wrap(done), function() {
      that.instance.publish("hello");
    });
  });

  it('should get the redis client for subscribing already created', function(done) {
    var opts = redisSettings();
    var initialConnection = opts.redis.createClient(opts.port, opts.host, opts);
    opts.sub_conn = initialConnection;
    var that = this;
    that.instance = new ascoltatori.RedisAscoltatore(opts);
    that.instance.subscribe("hello", wrap(done), function() {
      that.instance.publish("hello");
    });
  });
});
