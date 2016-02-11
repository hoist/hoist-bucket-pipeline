'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol ? "symbol" : typeof obj; };

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _errors = require('@hoist/errors');

var _errors2 = _interopRequireDefault(_errors);

var _logger = require('@hoist/logger');

var _logger2 = _interopRequireDefault(_logger);

var _model = require('@hoist/model');

var _lodash = require('lodash');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * Pipeline class for interacting with Buckets
 */

var BucketPipeline = function () {
  /**
   * create a new Pipeline
   */

  function BucketPipeline() {
    _classCallCheck(this, BucketPipeline);

    this._logger = _logger2.default.child({
      cls: this.constructor.name
    });
  }

  _createClass(BucketPipeline, [{
    key: '_addHelper',
    value: function _addHelper(context, key, meta) {
      var _this = this;

      return Promise.resolve().then(function () {
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
        var newBucket = _this._createBucket(options);
        return newBucket.saveAsync().then(function (bucket) {
          return bucket.toObject();
        });
      });
    }
  }, {
    key: '_createBucket',
    value: function _createBucket(options) {
      return new _model.Bucket(options);
    }
  }, {
    key: '_saveMetaHelper',
    value: function _saveMetaHelper(bucket, meta) {
      if (bucket) {
        if (bucket.meta) {
          bucket.meta = (0, _lodash.extend)(bucket.meta, meta);
        } else {
          bucket.meta = meta;
        }
        bucket.markModified('meta');
        return bucket.saveAsync().then(function (results) {
          return results[0].meta;
        });
      }
      throw new _errors2.default.bucket.NotFoundError();
    }

    /**
     * add a new bucket and set the meta data
     * @param {Context} context - the current context
     * @param {String} key - the unique key for the bucket
     * @param {Object} [meta] - any mata data to save
     * @returns {Promise<Object>} - the Bucket in object form
     */

  }, {
    key: 'add',
    value: function add(context, key, meta) {
      var _this2 = this;

      return Promise.resolve().then(function () {
        if (!meta && (typeof key === 'undefined' ? 'undefined' : _typeof(key)) === 'object') {
          meta = key;
          key = null;
        }
        if (key) {
          _this2._logger.info('resolving context');

          _this2._logger.info({
            context: context
          }, 'looking up bucket');
          return _model.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              throw new _errors2.default.bucket.InvalidError('A bucket with key "' + key + '" already exists');
            }
            return _this2._addHelper(context, key, meta);
          });
        }
        return _this2._addHelper(context, key, meta);
      });
    }

    /**
     * load the bucket specified from the database or create it
     * and set it as the current bucket
     * @param {Context} context - the current context
     * @param {String} key - the unique key for the bucket
     * @param {Boolean} [create=false] - should we create the bucket?
     * @returns {Promise<Object>} - the Bucket in object form
     */

  }, {
    key: 'set',
    value: function set(context, key, create) {
      var _this3 = this;

      var self = this;
      this._logger.info('resolving context');
      return Promise.resolve().then(function () {
        _this3._logger.info({
          context: context
        }, 'looking up bucket');
        return _model.Bucket.findOneAsync({
          key: key,
          environment: context.environment,
          application: context.application._id
        }).then(function (bucket) {
          _this3._logger.info({
            bucket: bucket
          }, 'bucket lookup complete');
          if (bucket) {
            return context.bucket = bucket.toObject();
          }
          if (create) {
            return self.add(key).then(function (addedBucket) {
              if (addedBucket) {
                //add returns an object so don't call to object here
                return context.bucket = addedBucket;
              }
              throw new _errors2.default.bucket.SaveError();
            });
          }
          throw new _errors2.default.bucket.NotFoundError();
        });
      });
    }

    /**
     * get a new bucket from the database
     * @param {Context} context - the current context
     * @param {String} key - the unique key for the bucket
     * @returns {Promise<Object>} - the Bucket in object form
     */

  }, {
    key: 'get',
    value: function get(context, key) {
      var _this4 = this;

      this._logger.info('getting bucket');
      this._logger.info('retrieving context');
      return Promise.resolve().then(function () {
        if (key) {
          _this4._logger.info('loading bucket by key');
          return _model.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              return bucket.toObject();
            }
            throw new _errors2.default.bucket.NotFoundError();
          });
        }
        if (context.bucket) {
          _this4._logger.info('loading from context');
          return _model.Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              _this4._logger.info('retrieved bucket');
              return bucket.toObject();
            }
            _this4._logger.info('no bucket found');
            throw new _errors2.default.bucket.NotFoundError();
          });
        }
        return null;
      });
    }

    /**
     * remove bucket from the database
     * @param {Context} context - the current context
     * @param {String} key - the unique key for the bucket
     * @returns {Promise} - the Promise to have deleted the bucket
     */

  }, {
    key: 'remove',
    value: function remove(context, key) {
      var _this5 = this;

      this._logger.info('remove bucket');
      this._logger.info('getting context');
      return Promise.resolve().then(function () {
        if (key) {
          _this5._logger.info('removing bucket by key');
          return _model.Bucket.removeAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function () {

            if (context.bucket && context.bucket.key === key) {
              _this5._logger.info('removing bucket from context');
              context.bucket = null;
            }
          });
        }
        if (context.bucket) {
          _this5._logger.info('removing bucket by context');
          return _model.Bucket.removeAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function () {
            _this5._logger.info('removing bucket from context');
            context.bucket = null;
          });
        }
        return null;
      });
    }

    /**
     * get all application buckets from the database
     * @param {Context} context - the current context
     * @returns {Promise<Array<Object>>} - an Array of Buckets in object form
     */

  }, {
    key: 'getAll',
    value: function getAll(context) {
      var _this6 = this;

      this._logger.info('getting all buckets');
      return Promise.resolve().then(function () {
        _this6._logger.info('loaded context');
        return _model.Bucket.findAsync({
          environment: context.environment,
          application: context.application._id
        }).then(function (buckets) {
          _this6._logger.info({
            bucketCount: buckets.length
          }, 'returning buckets');
          return buckets.map(function (bucket) {

            return bucket.toObject();
          });
        });
      });
    }

    /**
     * run a function over every bucket in the organisation
     * @param {Context} context - the current context
     * @param {function(Bucket: bucket)} fn - the function to run
     * @returns {Promise} - promise to have run the function over each bucket
     */

  }, {
    key: 'each',
    value: function each(context, fn) {
      var _this7 = this;

      this._logger.info('calling each');
      return this.getAll(context).then(function (buckets) {
        _this7._logger.info('running function against each bucket');
        return Promise.all(buckets.map(function (bucket) {
          _this7._logger.info({
            bucketId: bucket._id
          }, 'running function against a bucket');
          return Promise.resolve(fn(bucket));
        }));
      });
    }

    /**
     * sets and replaces meta data against a bucket
     * @param {Context} context - the current context
     * @param {Object} meta - the mata data to save
     * @param {String} [key] - the unique key for the bucket, if not set use the current context bucket
     * @returns {Promise<Object>} - the Bucket in object form
     */

  }, {
    key: 'saveMeta',
    value: function saveMeta(context, meta, key) {
      var _this8 = this;

      this._logger.info('saving meta data');
      this._logger.info('getting context');
      return Promise.resolve().then(function () {
        _this8._logger.info('retrieved context');
        if (key) {
          _this8._logger.info('finding bucket by key');
          return _model.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            _this8._logger.info('saving meta data to bucket');
            return _this8._saveMetaHelper(bucket, meta);
          });
        }
        if (context.bucket) {
          _this8._logger.info('finding bucket by context');
          return _model.Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            _this8._logger.info('saving meta data to bucket');
            return _this8._saveMetaHelper(bucket, meta);
          });
        }
        _this8._logger.info('no bucket found');
        throw new _errors2.default.bucket.NotFoundError();
      });
    }
  }]);

  return BucketPipeline;
}();

exports.default = BucketPipeline;

/**
 * @external {Context} https://github.com/hoist/hoist-context/blob/feature/remove_cls/src/index.js
 */
//# sourceMappingURL=pipeline.js.map
