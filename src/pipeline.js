'use strict';
import Errors from '@hoist/errors';
import logger from '@hoist/logger';
import {
  Bucket
}
from '@hoist/model';

import {
  extend
}
from 'lodash';

/**
 * Pipeline class for interacting with Buckets
 */
class BucketPipeline {
  /**
   * create a new Pipeline
   */
  constructor() {
    this._logger = logger.child({
      cls: this.constructor.name
    });
  }
  _addHelper(context, key, meta) {
    return Promise.resolve().then(() => {
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
      var newBucket = this._createBucket(options);
      return newBucket.saveAsync().then((bucket) => {
        return bucket.toObject();
      });
    });
  }
  _createBucket(options) {
    return new Bucket(options);
  }
  _saveMetaHelper(bucket, meta) {
    if (bucket) {
      if (bucket.meta) {
        bucket.meta = extend(bucket.meta, meta);
      } else {
        bucket.meta = meta;
      }
      bucket.markModified('meta');
      return bucket.saveAsync().then((results) => {
        return results.meta;
      });
    }
    throw new Errors.bucket.NotFoundError();
  }

  /**
   * add a new bucket and set the meta data
   * @param {Context} context - the current context
   * @param {String} key - the unique key for the bucket
   * @param {Object} [meta] - any mata data to save
   * @returns {Promise<Object>} - the Bucket in object form
   */
  add(context, key, meta) {
    return Promise.resolve()
      .then(() => {
        if (!meta && typeof key === 'object') {
          meta = key;
          key = null;
        }
        if (key) {
          this._logger.info('resolving context');

          this._logger.info({
            context: context
          }, 'looking up bucket');
          return Bucket.findOneAsync({
              key: key,
              environment: context.environment,
              application: context.application._id
            })
            .then((bucket) => {
              if (bucket) {
                throw new Errors.bucket.InvalidError('A bucket with key "' + key + '" already exists');
              }
              return this._addHelper(context, key, meta);
            });
        }
        return this._addHelper(context, key, meta);
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
  set(context, key, create) {
    var self = this;
    this._logger.info('resolving context');
    return Promise.resolve()
      .then(() => {
        this._logger.info({
          context: context
        }, 'looking up bucket');
        return Bucket.findOneAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          })
          .then((bucket) => {
            this._logger.info({
              bucket: bucket
            }, 'bucket lookup complete');
            if (bucket) {
              return (context.bucket = bucket.toObject());
            }
            if (create) {
              return self.add(key).then((addedBucket) => {
                if (addedBucket) {
                  //add returns an object so don't call to object here
                  return (context.bucket = addedBucket);
                }
                throw new Errors.bucket.SaveError();
              });
            }
            throw new Errors.bucket.NotFoundError();
          });
      });
  }

  /**
   * get a new bucket from the database
   * @param {Context} context - the current context
   * @param {String} key - the unique key for the bucket
   * @returns {Promise<Object>} - the Bucket in object form
   */
  get(context, key) {
    this._logger.info('getting bucket');
    this._logger.info('retrieving context');
    return Promise.resolve()
      .then(() => {
        if (key) {
          this._logger.info('loading bucket by key');
          return Bucket.findOneAsync({
              key: key,
              environment: context.environment,
              application: context.application._id
            })
            .then((bucket) => {
              if (bucket) {
                return bucket.toObject();
              }
              throw new Errors.bucket.NotFoundError();
            });
        }
        if (context.bucket) {
          this._logger.info('loading from context');
          return Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then((bucket) => {
            if (bucket) {
              this._logger.info('retrieved bucket');
              return bucket.toObject();
            }
            this._logger.info('no bucket found');
            throw new Errors.bucket.NotFoundError();
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
  remove(context, key) {
    this._logger.info('remove bucket');
    this._logger.info('getting context');
    return Promise.resolve()
      .then(() => {
        if (key) {
          this._logger.info('removing bucket by key');
          return Bucket.removeAsync({
            key: key,
            environment: context.environment,
            application: context.application._id
          }).then(() => {

            if (context.bucket && context.bucket.key === key) {
              this._logger.info('removing bucket from context');
              context.bucket = null;
            }
          });
        }
        if (context.bucket) {
          this._logger.info('removing bucket by context');
          return Bucket.removeAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then(() => {
            this._logger.info('removing bucket from context');
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
  getAll(context) {
    this._logger.info('getting all buckets');
    return Promise.resolve()
      .then(() => {
        this._logger.info('loaded context');
        return Bucket.findAsync({
          environment: context.environment,
          application: context.application._id
        }).then((buckets) => {
          this._logger.info({
            bucketCount: buckets.length
          }, 'returning buckets');
          return buckets.map((bucket) => {

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
  each(context, fn) {
    this._logger.info('calling each');
    return this.getAll(context).then((buckets) => {
      this._logger.info('running function against each bucket');
      return Promise.all(buckets.map((bucket) => {
        this._logger.info({
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
  saveMeta(context, meta, key) {
    this._logger.info('saving meta data');
    this._logger.info('getting context');
    return Promise.resolve()
      .then(() => {
        this._logger.info('retrieved context');
        if (key) {
          this._logger.info('finding bucket by key');
          return Bucket.findOneAsync({
              key: key,
              environment: context.environment,
              application: context.application._id
            })
            .then((bucket) => {
              this._logger.info('saving meta data to bucket');
              return this._saveMetaHelper(bucket, meta);
            });
        }
        if (context.bucket) {
          this._logger.info('finding bucket by context');
          return Bucket.findOneAsync({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          }).then((bucket) => {
            this._logger.info('saving meta data to bucket');
            return this._saveMetaHelper(bucket, meta);
          });
        }
        this._logger.info('no bucket found');
        throw new Errors.bucket.NotFoundError();
      });
  }
}

export default BucketPipeline;

/**
 * @external {Context} https://github.com/hoist/hoist-context/blob/feature/remove_cls/src/index.js
 */
