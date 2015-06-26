'use strict';
Object.defineProperty(exports, '__esModule', {
  value: true
});

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _hoistErrors = require('@hoist/errors');

var _hoistErrors2 = _interopRequireDefault(_hoistErrors);

var _hoistLogger = require('@hoist/logger');

var _hoistLogger2 = _interopRequireDefault(_hoistLogger);

var _hoistContext = require('@hoist/context');

var _hoistContext2 = _interopRequireDefault(_hoistContext);

var _hoistModel = require('@hoist/model');

var _lodash = require('lodash');

/**
 * Pipeline class for interacting with Buckets
 */

var Pipeline = (function () {
  /**
   * create a new Pipeline
   */

  function Pipeline() {
    _classCallCheck(this, Pipeline);

    this._logger = _hoistLogger2['default'].child({
      cls: this.constructor.name
    });
  }

  _createClass(Pipeline, [{
    key: '_addHelper',
    value: function _addHelper(key, meta) {
      var _this = this;

      return _hoistContext2['default'].get().then(function (context) {
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
      return new _hoistModel.Bucket(options);
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
      throw new _hoistErrors2['default'].bucket.NotFoundError();
    }
  }, {
    key: 'add',

    /**
     * add a new bucket and set the meta data
     * @param {String} key - the unique key for the bucket
     * @param {Object} [meta] - any mata data to save
     * @returns {Promise<Object>} - the Bucket in object form
     */
    value: function add(key, meta) {
      var _this2 = this;

      if (!meta && typeof key === 'object') {
        meta = key;
        key = null;
      }
      if (key) {
        this._logger.info('resolving context');
        return _hoistContext2['default'].get().then(function (context) {
          _this2._logger.info({
            context: context
          }, 'looking up bucket');
          return _hoistModel.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              throw new _hoistErrors2['default'].bucket.InvalidError('A bucket with key "' + key + '" already exists');
            }
            return _this2._addHelper(key, meta);
          });
        });
      }
      return this._addHelper(key, meta);
    }
  }, {
    key: 'set',

    /**
     * load the bucket specified from the database or create it
     * and set it as the current bucket
     * @param {String} key - the unique key for the bucket
     * @param {Boolean} [create=false] - should we create the bucket?
     * @returns {Promise<Object>} - the Bucket in object form
     */
    value: function set(key, create) {
      var _this3 = this;

      var self = this;
      this._logger.info('resolving context');
      return _hoistContext2['default'].get().then(function (context) {
        _this3._logger.info({
          context: context
        }, 'looking up bucket');
        return _hoistModel.Bucket.findOneAsync({
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
              throw new _hoistErrors2['default'].bucket.SaveError();
            });
          }
          throw new _hoistErrors2['default'].bucket.NotFoundError();
        });
      });
    }
  }, {
    key: 'get',

    /**
     * get a new bucket from the database
     * @param {String} key - the unique key for the bucket
     * @returns {Promise<Object>} - the Bucket in object form
     */
    value: function get(key) {
      return _hoistContext2['default'].get().then(function (context) {
        if (key) {
          return _hoistModel.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              return bucket.toObject();
            }
            throw new _hoistErrors2['default'].bucket.NotFoundError();
          });
        }
        if (context.bucket) {
          return _hoistModel.Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            if (bucket) {
              return bucket.toObject();
            }
            throw new _hoistErrors2['default'].bucket.NotFoundError();
          });
        }
        return null;
      });
    }
  }, {
    key: 'remove',

    /**
     * remove bucket from the database
     * @param {String} key - the unique key for the bucket
     * @returns {Promise} - the Promise to have deleted the bucket
     */
    value: function remove(key) {
      return _hoistContext2['default'].get().then(function (context) {
        if (key) {
          return _hoistModel.Bucket.removeAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function () {
            if (context.bucket && context.bucket.key === key) {
              context.bucket = null;
            }
          });
        }
        if (context.bucket) {
          return _hoistModel.Bucket.removeAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function () {
            context.bucket = null;
          });
        }
        return null;
      });
    }
  }, {
    key: 'getAll',

    /**
     * get all application buckets from the database
     * @returns {Promise<Array<Object>>} - an Array of Buckets in object form
     */
    value: function getAll() {
      return _hoistContext2['default'].get().then(function (context) {
        return _hoistModel.Bucket.findAsync({
          environment: context.environment,
          application: context.application._id
        }).then(function (buckets) {
          return buckets.map(function (bucket) {
            return bucket.toObject();
          });
        });
      });
    }
  }, {
    key: 'each',

    /**
     * run a function over every bucket in the organisation
     * @param {function(Bucket: bucket)} fn - the function to run
     * @returns {Promise} - promise to have run the function over each bucket
     */
    value: function each(fn) {
      return this.getAll().then(function (buckets) {
        return Promise.all(buckets.map(function (bucket) {
          return Promise.resolve(fn(bucket));
        }));
      });
    }
  }, {
    key: 'saveMeta',

    /**
     * sets and replaces meta data against a bucket
     * @param {Object} meta - the mata data to save
     * @param {String} [key] - the unique key for the bucket, if not set use the current context bucket
     * @returns {Promise<Object>} - the Bucket in object form
     */
    value: function saveMeta(meta, key) {
      var _this4 = this;

      return _hoistContext2['default'].get().then(function (context) {
        if (key) {
          return _hoistModel.Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            return _this4._saveMetaHelper(bucket, meta);
          });
        }
        if (context.bucket) {
          return _hoistModel.Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(function (bucket) {
            return _this4._saveMetaHelper(bucket, meta);
          });
        }
        throw new _hoistErrors2['default'].bucket.NotFoundError();
      });
    }
  }]);

  return Pipeline;
})();

exports['default'] = Pipeline;
module.exports = exports['default'];
//# sourceMappingURL=pipeline.js.map