'use strict';
var HoistErrors = require('hoist-errors');

function Pipeline(Context, Model) {
  this.Context = Context || require('hoist-context');
  this.Model = Model || require('hoist-model');
}

Pipeline.prototype.add = function add() {
  var bucketArgs = Array.prototype.slice.call(arguments, 0);
  var meta = null;
  var key = null;
  meta = (bucketArgs.length > 1 && ((typeof (bucketArgs[bucketArgs.length - 1])) === 'object') ) ? bucketArgs.splice(-1, 1)[0] : meta;
  key = (bucketArgs.length > 1 && ((typeof (bucketArgs[bucketArgs.length - 1])) === 'string')) ? bucketArgs.splice(-1, 1)[0] : key;
  key = (bucketArgs.length === 1 && ((typeof (bucketArgs[0])) === 'string') ) ? bucketArgs[0] : key;
  meta = (bucketArgs.length === 1 && ((typeof (bucketArgs[0])) === 'object')) ? bucketArgs[0] : meta;
  if (key !== null ) {
    return this.Context.get().bind(this).then(function (context) { 
      return this.Model.Bucket.findOneAsync({key: key, environment: context.environment, application: context.application})
      .bind(this)
      .then(function (bucket) {
        if (bucket) {
          throw new HoistErrors.bucket.InvalidError('A bucket with key "'+key+'" already exists');
        }
        return this.addHelper(key, meta);
      });
    });
  } 
  key = null;
  return this.addHelper(key, meta);
};

Pipeline.prototype.addHelper = function (key, meta) {
  return this.Context.get().bind(this).then(function (context) {
    var options = {
      application: context.application,
      environment: context.environment
    };
    if (key) {
      options._id = key;
    }
    if (meta) {
      options.meta = meta;
    }
    var newBucket = this.createBucket(options);
    return newBucket.saveAsync();
  });
};
/* istanbul ignore next */
Pipeline.prototype.createBucket = function(options) {
  return new this.Model.Bucket(options);
};

Pipeline.prototype.setCurrent = function (key) {
  return this.Context.get().bind(this).then(function (context) { 
    return this.Model.Bucket.findOneAsync({key: key, environment: context.environment, application: context.application})
    .then(function (bucket) {
      if (bucket) {
        return (context.bucket = key);
      }
      throw new HoistErrors.bucket.NotFoundError();
    });
  });
};
/* istanbul ignore next */
module.exports = function (hoistContext, Model) {
  return new Pipeline(hoistContext, Model);
};
module.exports.Pipeline = Pipeline;