'use strict';
var HoistErrors = require('hoist-errors');
var BBPromise = require('bluebird');
var _ = require('lodash');

function Pipeline(Context, Model) {
  this.Context = Context || require('hoist-context');
  this.Model = Model || require('hoist-model');
}

Pipeline.prototype.add = function add(key, meta) {
  if(!meta && typeof key === 'object'){
    meta = key;
    key = null;
  } 
  if (key) {
    return this.Context.get().bind(this).then(function (context) { 
      return this.Model.Bucket.findOneAsync({key: key, environment: context.environment, application: context.application._id})
      .bind(this)
      .then(function (bucket) {
        if (bucket) {
          throw new HoistErrors.bucket.InvalidError('A bucket with key "'+key+'" already exists');
        }
        return this.addHelper(key, meta);
      });
    });
  }
  return this.addHelper(key, meta);
};

Pipeline.prototype.addHelper = function (key, meta) {
  return this.Context.get().bind(this).then(function (context) {
    var options = {
      application: context.application._id,
      environment: context.environment
    };
    if (key) {
      options.key = key;
    }
    if (meta) {
      options.meta = meta;
    } else {
      options.meta = {};
    }
    var newBucket = this.createBucket(options);
    return newBucket.saveAsync().then(function (bucket) {
      return bucket.toObject();
    });
  });
};
/* istanbul ignore next */
Pipeline.prototype.createBucket = function(options) {
  return new this.Model.Bucket(options);
};

Pipeline.prototype.set = function (key, create) {
  var self = this;
  return this.Context.get().bind(this).then(function (context) { 
    return this.Model.Bucket.findOneAsync({key: key, environment: context.environment, application: context.application._id})
    .then(function (bucket) {
      if (bucket) {
        return (context.bucket = bucket.toObject());
      }
      if (create) {
        return self.add(key).then(function (bucket) {
          if (bucket) {
            return (context.bucket = bucket.toObject());
          }
          throw new HoistErrors.bucket.SaveError();
        });
      }
      throw new HoistErrors.bucket.NotFoundError();
    });
  });
};

Pipeline.prototype.get = function (key) {
  return this.Context.get().bind(this).then(function (context) {
    if (key) {
      return this.Model.Bucket.findOneAsync({
          key: key,
          environment: context.environment,
          application: context.application._id
        })
        .then(function (bucket) {
          if (bucket) {
            return bucket.toObject();
          }
          throw new HoistErrors.bucket.NotFoundError();
        });
    }
    if(context.bucket){
      return this.Model.Bucket.findOneAsync({
        key: context.bucket.key,
        environment: context.environment,
        application: context.application._id
      }).then(function(bucket){
        if (bucket) {
          return bucket.toObject();
        }
        throw new HoistErrors.bucket.NotFoundError();
      });
    }
    return null;
  });
};

Pipeline.prototype.getAll = function () {
  return this.Context.get().bind(this).then(function (context) {
    return this.Model.Bucket.findAsync({
      environment: context.environment,
      application: context.application._id
    }).map(function(bucket){
      return bucket.toObject();
    });
  });
};

Pipeline.prototype.each = function (fn) {
  return this.getAll().then(function(buckets){
    return BBPromise.all(_.map(buckets, function(bucket){
      return BBPromise.resolve(fn(bucket));
    }));
  });
};

function saveMetaHelper(bucket, meta) {
  if (bucket) {
    if (bucket.meta) {
      bucket.meta = _.extend(bucket.meta, meta);
    } else {
      bucket.meta = meta;
    }
    bucket.markModified('meta');
    return bucket.saveAsync().spread(function (savedBucket) {
      return savedBucket.meta;
    });
  }
  throw new HoistErrors.bucket.NotFoundError();
}

Pipeline.prototype.saveMeta = function (meta, key) {
  return this.Context.get().bind(this).then(function (context) {
    if (key) {
      return this.Model.Bucket.findOneAsync({
          key: key,
          environment: context.environment,
          application: context.application._id
        })
        .then(function (bucket) {
          return saveMetaHelper(bucket, meta);
        });
    }
    if (context.bucket) {
      return this.Model.Bucket.findOneAsync({
        key: context.bucket.key,
        environment: context.environment,
        application: context.application._id
      }).then(function (bucket) {
        return saveMetaHelper(bucket, meta);
      });
    }
    throw new HoistErrors.bucket.NotFoundError();
  });
};

/* istanbul ignore next */
module.exports = function (hoistContext, Model) {
  return new Pipeline(hoistContext, Model);
};
module.exports.Pipeline = Pipeline;