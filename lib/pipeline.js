'use strict';
var Bucket = require('hoist-model').Bucket;

function Pipeline(Context, Model) {
  this.Context = Context || require('hoist-context');
  this.Model = Model || require('hoist-model');
}

Pipeline.prototype.add = function add(key, meta) {
  return this.Context.get().bind(this).then(function (context) {
    var options = {
      application: context.application,
      environment: context.environment
    };
    options._id = (key) ?  key : null;
    options.meta = (meta) ?  meta : null;
    this.newBucket = new Bucket(options);
    return this.newBucket.saveAsync().then(function (bucket) {
      return bucket;
    });
  });
};

module.exports = function (hoistContext, Model) {
  return new Pipeline(hoistContext, Model);
};
module.exports.Pipeline = Pipeline;