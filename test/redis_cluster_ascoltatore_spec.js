var fs = require("fs");

/*describeAscoltatore("redisCluster", function() {

  afterEach(function() {
    this.instance.close();
  });

  it("should publish a binary payload into redis-cluster", function(done) {
    this.instance.close();
    var that=this;
    var settings = redisClusterSettings();
    settings.settings.redis.detect_buffers = true;

    that.instance = new ascoltatori.RedisClusterAscoltatore(settings);
    var expected = fs.readFileSync(__dirname + "/image.png");

    that.instance.sub("image", function(topic, value) {
      expect(value).to.eql(expected);
      done();
    }, function() {
      that.instance.pub("image", expected);
    });

  });

  it("should sync two redis-cluster instances", function(done) {
    var other = new ascoltatori.RedisClusterAscoltatore(redisClusterSettings());
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

  it('should accept a already created redis-cluster passed on the opts', function(done) {
    var opts = redisClusterSettings();
    opts._rc = new opts.redisCluster(opts.nodes,opts.settings);
    var that = this;
    that.instance = new ascoltatori.RedisClusterAscoltatore(opts);
    that.instance.subscribe("hello", wrap(done), function() {
      that.instance.publish("hello");
    });
  });
});*/
