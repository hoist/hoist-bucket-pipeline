'use strict';

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
    if (key) {
      options._id = key;
    }
    if (meta) {
      options.meta = meta;
    }
    this.newBucket = this.createBucket(options);
    return this.newBucket.saveAsync();
  });
};

Pipeline.prototype.createBucket = function(options) {
  return new this.Model.Bucket(options);
};

module.exports = function (hoistContext, Model) {
  return new Pipeline(hoistContext, Model);
};
module.exports.Pipeline = Pipeline;