'use strict';
import BucketPipeline from '../../src/pipeline';
import {
  expect
}
from 'chai';
import sinon from 'sinon';
import {
  Bucket
}
from '@hoist/model';
import Errors from '@hoist/errors';
import Context from '@hoist/context';

describe('bucketPipeline', function () {
  describe('.add', function () {
    describe('with no arguments', function () {
      var bucket = {
        toObject: function () {
          return this;
        }
      };
      var newBucketPipeline;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
        sinon.stub(BucketPipeline.prototype, '_addHelper').returns(Promise.resolve(bucket));
        newBucketPipeline = new BucketPipeline();
        return newBucketPipeline.add(undefined, undefined);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        BucketPipeline.prototype._addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(BucketPipeline.prototype._addHelper.calledWith(undefined, undefined)).to.be.true;
      });
    });
    describe('with valid key argument', function () {
      var newBucketPipeline;
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey,
        toObject: function () {
          return this;
        }
      };
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve());
        sinon.stub(BucketPipeline.prototype, '_addHelper').returns(Promise.resolve(bucket));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return newBucketPipeline.add(fakeKey, undefined);
      });
      after(function () {
        Context.get.restore();
        Bucket.findOneAsync.restore();
        BucketPipeline.prototype._addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(BucketPipeline.prototype._addHelper.calledWith(fakeKey, undefined)).to.eql(true);
      });
    });
    describe('with a duplicate key argument', function () {
      var bucket = {
        toObject: function () {
          return this;
        }
      };
      var newBucketPipeline;
      var fakeKey = 'fake key';
      var error;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
        sinon.stub(BucketPipeline.prototype, '_addHelper').returns(Promise.resolve());
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        newBucketPipeline.add(fakeKey, undefined).catch(function (err) {
          error = err;
          done();
        });
      });
      after(function () {
        Context.get.restore();
        Bucket.findOneAsync.restore();
        BucketPipeline.prototype._addHelper.restore();
      });
      it('rejects', function () {
        expect(error)
          .to.be.instanceOf(Errors.bucket.InvalidError)
          .and.have.property('message', 'A bucket with key "' + fakeKey + '" already exists');
      });

    });
    describe('with valid meta argument', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakeKey: 'fake data'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve());
        newBucketPipeline = new BucketPipeline();
        sinon.stub(BucketPipeline.prototype, '_addHelper').returns(Promise.resolve());
        return newBucketPipeline.add(fakeMeta, undefined);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        BucketPipeline.prototype._addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(BucketPipeline.prototype._addHelper.calledWith(null, fakeMeta)).to.eql(true);
      });
      it('doesn\'t call Bucket.findOneAsync', function () {
        return expect(Bucket.findOneAsync.called).to.eql(false);
      });
    });
    describe('with valid key and meta arguments, with key first', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakeKey: 'fake data'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve());
        sinon.stub(BucketPipeline.prototype, '_addHelper').returns(Promise.resolve());
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return newBucketPipeline.add(fakeKey, fakeMeta);
      });
      after(function () {
        Context.get.restore();
        Bucket.findOneAsync.restore();
        BucketPipeline.prototype._addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(BucketPipeline.prototype._addHelper.calledWith(fakeKey, fakeMeta)).to.eql(true);
      });
    });
  });

  describe('._addHelper', function () {
    describe('with key and meta', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        key: 'pthfut76-7ehfgdt23sw',
        meta: {
          testMeta: 'test'
        },
        application: 'application',
        environment: 'environment'
      };
      before(function () {
        bucket = {
          toObject: function () {
            return this;
          }
        };
        newBucketPipeline = new BucketPipeline();
        bucket.saveAsync = sinon.stub().returns(Promise.resolve(bucket));
        sinon.stub(newBucketPipeline, '_createBucket').returns(bucket);
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return (result = newBucketPipeline._addHelper('pthfut76-7ehfgdt23sw', {
          testMeta: 'test'
        }));
      });
      after(function () {
        Context.get.restore();
        newBucketPipeline._createBucket.restore();
      });
      it('calls hoist-context', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('creates a new bucket with correct args', function () {
        return expect(newBucketPipeline._createBucket.calledWith(options)).to.eql(true);
      });

      it('saves the new bucket', function () {
        return expect(bucket.saveAsync.calledOnce).to.eql(true);
      });
      it('returns the new bucket', function () {
        expect(result).to.become(bucket);
      });
    });
    describe('with key and no meta', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        key: 'pthfut76-7ehfgdt23sw',
        meta: {},
        application: 'application',
        environment: 'environment'
      };
      before(function () {
        bucket = {
          toObject: function () {
            return this;
          }
        };
        newBucketPipeline = new BucketPipeline();
        bucket.saveAsync = sinon.stub().returns(Promise.resolve(bucket));
        sinon.stub(newBucketPipeline, '_createBucket').returns(bucket);
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return (result = newBucketPipeline._addHelper('pthfut76-7ehfgdt23sw'));
      });
      after(function () {
        Context.get.restore();
        newBucketPipeline._createBucket.restore();
      });
      it('calls hoist-context', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('creates a new bucket with correct args', function () {
        return expect(newBucketPipeline._createBucket.calledWith(options)).to.eql(true);
      });

      it('saves the new bucket', function () {
        return expect(bucket.saveAsync.calledOnce).to.eql(true);
      });
      it('returns the new bucket', function () {
        expect(result).to.become(bucket);
      });
    });
    describe('with no key and meta', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        application: 'application',
        environment: 'environment',
        meta: {}
      };
      before(function () {
        bucket = {
          toObject: function () {
            return this;
          }
        };
        newBucketPipeline = new BucketPipeline();
        bucket.saveAsync = sinon.stub().returns(Promise.resolve(bucket));
        sinon.stub(newBucketPipeline, '_createBucket').returns(bucket);
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return (result = newBucketPipeline._addHelper(undefined, undefined));
      });
      after(function () {
        Context.get.restore();
        newBucketPipeline._createBucket.restore();
      });
      it('calls hoist-context', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('creates a new bucket with correct args', function () {
        return expect(newBucketPipeline._createBucket.calledWith(options)).to.eql(true);
      });

      it('saves the new bucket', function () {
        return expect(bucket.saveAsync.calledOnce).to.eql(true);
      });
      it('returns the new bucket', function () {
        expect(result).to.become(bucket);
      });
    });
  });

  describe('.set', function () {
    describe('with a existing bucket key', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey,
        toObject: function () {
          return {
            key: fakeKey
          };
        }
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return newBucketPipeline.set(fakeKey);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('sets context.bucket to the bucket key', function () {
        return expect(context.bucket).to.eql({
          key: fakeKey
        });
      });
    });
    describe('with a non existing bucket key and create true', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey,
        toObject: function () {
          return {
            key: fakeKey
          };
        }
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(null));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        sinon.stub(newBucketPipeline, 'add').returns(Promise.resolve(bucket.toObject()));
        return newBucketPipeline.set(fakeKey, true);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
        newBucketPipeline.add.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('calls bucketPipeline.add', function () {
        return expect(newBucketPipeline.add.calledWith(fakeKey)).to.eql(true);
      });
      it('sets context.bucket to the bucket key', function () {
        return expect(context.bucket).to.eql({
          key: fakeKey
        });
      });
    });
    describe('with a non existing bucket key and create false', function () {
      var newBucketPipeline;
      var error;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(null));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        newBucketPipeline.set(fakeKey).catch(function (err) {
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('rejects', function () {
        return expect(error)
          .to.be.instanceOf(Errors.bucket.NotFoundError);
      });
      it('calls Context.get', function () {
        return expect(Context.get.called).to.eql(true);
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
    });
  });

  describe('.get', function () {
    describe('with a existing bucket key', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey,
        toObject: function () {
          return this;
        }
      };
      var result;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return (result = newBucketPipeline.get(fakeKey));
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('returns bucket', function () {
        return expect(result).to.become(bucket);
      });
    });
    describe('with a non existing bucket key', function () {
      var newBucketPipeline;
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var error;
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(null));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return newBucketPipeline.get(fakeKey).catch(function (err) {
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('throws bucket not found error', function () {
        return expect(error)
          .to.be.instanceOf(Errors.bucket.NotFoundError);
      });
    });
    describe('with no key', function () {
      describe('with context.bucket', function () {
        var newBucketPipeline;
        var fakeKey = '2hgjfkitl98-6_hftgh4';
        var context = {
          application: {
            _id: 'application'
          },
          bucket: {
            key: fakeKey
          },
          environment: 'environment'
        };
        var bucket = {
          key: fakeKey,
          toObject: function () {
            return this;
          }
        };
        var result;
        before(function () {
          sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
          newBucketPipeline = new BucketPipeline();
          sinon.stub(Context, 'get').returns(Promise.resolve(context));
          return (result = newBucketPipeline.get());
        });
        after(function () {
          Bucket.findOneAsync.restore();
          Context.get.restore();
        });
        it('calls bucket.findOneAsync with correct args', function () {
          return expect(Bucket.findOneAsync.calledWith({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          })).to.eql(true);
        });
        it('calls Context.get', function () {
          return expect(Context.get.calledOnce).to.eql(true);
        });
        it('returns bucket', function () {
          return expect(result).to.become(bucket);
        });
      });
      describe('with no context.bucket', function () {
        var newBucketPipeline;
        var context = {
          application: {
            _id: 'application'
          },
          environment: 'environment'
        };
        var result;
        before(function () {
          sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve());
          newBucketPipeline = new BucketPipeline();
          sinon.stub(Context, 'get').returns(Promise.resolve(context));
          return (result = newBucketPipeline.get());
        });
        after(function () {
          Bucket.findOneAsync.restore();
          Context.get.restore();
        });
        it('does not call bucket.findOneAsync', function () {
          return expect(Bucket.findOneAsync.called).to.eql(false);
        });
        it('calls Context.get', function () {
          return expect(Context.get.calledOnce).to.eql(true);
        });
        it('returns bucket', function () {
          return expect(result).to.become(null);
        });
      });
    });
  });
  describe('.getAll', function () {
    var newBucketPipeline;
    var context = {
      application: {
        _id: 'application'
      },
      environment: 'environment'
    };
    var result;
    before(function () {
      sinon.stub(Bucket, 'findAsync').returns(Promise.resolve([]));
      newBucketPipeline = new BucketPipeline();
      sinon.stub(Context, 'get').returns(Promise.resolve(context));
      return (result = newBucketPipeline.getAll());
    });
    after(function () {
      Bucket.findAsync.restore();
      Context.get.restore();
    });
    it('calls bucket.findOneAsync with correct args', function () {
      return expect(Bucket.findAsync.calledWith({
        environment: context.environment,
        application: context.application._id
      })).to.eql(true);
    });
    it('calls Context.get', function () {
      return expect(Context.get.calledOnce).to.eql(true);
    });
    it('returns bucket', function () {
      return expect(result).to.become([]);
    });
  });
  describe('.each', function () {
    var newBucketPipeline;
    var buckets = [{
      key: 'key1'
    }, {
      key: 'key2'
    }];
    var result = [];
    before(function () {
      newBucketPipeline = new BucketPipeline();
      sinon.stub(newBucketPipeline, 'getAll').returns(Promise.resolve(buckets));
      return newBucketPipeline.each(function (bucket) {
        return result.push(bucket);
      });
    });
    after(function () {
      newBucketPipeline.getAll.restore();
    });
    it('calls getAll', function () {
      return expect(newBucketPipeline.getAll.calledOnce).to.eql(true);
    });
    it('calls function on each bucket', function () {
      return expect(result).to.eql(buckets);
    });
  });

  describe('.saveMeta', function () {
    describe('with a existing bucket key', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var meta = {
        key1: 'value',
        key: 'value'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey,
        meta: meta,
        toObject: function () {
          return {
            key: fakeKey,
            meta: meta
          };
        },
        markModified: function () {},
        saveAsync: sinon.stub().returns(Promise.resolve([{
          key: fakeKey,
          meta: meta
        }, 1]))
      };
      var newMeta = {
        key: 'newValue'
      };
      var result;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        return (result = newBucketPipeline.saveMeta(newMeta, fakeKey));
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(Context.get.calledOnce).to.eql(true);
      });
      it('sets bucket.meta', function () {
        return expect(result).to.become({
          key1: 'value',
          key: 'newValue'
        });
      });
    });
    describe('with a non existing bucket key', function () {
      var newBucketPipeline;
      var error;
      var context = {
        application: {
          _id: 'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var meta = {
        key: 'newValue'
      };
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(null));
        newBucketPipeline = new BucketPipeline();
        sinon.stub(Context, 'get').returns(Promise.resolve(context));
        newBucketPipeline.saveMeta(meta, fakeKey).catch(function (err) {
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        Context.get.restore();
      });
      it('rejects', function () {
        return expect(error)
          .to.be.instanceOf(Errors.bucket.NotFoundError);
      });
      it('calls Context.get', function () {
        return expect(Context.get.called).to.eql(true);
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({
          key: fakeKey,
          environment: context.environment,
          application: context.application._id
        })).to.eql(true);
      });
    });
    describe('with no key', function () {
      describe('with context.bucket', function () {
        var newBucketPipeline;
        var fakeKey = '2hgjfkitl98-6_hftgh4';
        var meta = {
          key1: 'value',
          key: 'value'
        };
        var bucket = {
          key: fakeKey,
          meta: meta,
          toObject: function () {
            return {
              key: fakeKey,
              meta: meta
            };
          },
          markModified: function () {},
          saveAsync: sinon.stub().returns(Promise.resolve([{
            key: fakeKey,
            meta: meta
          }, 1]))
        };
        var context = {
          application: {
            _id: 'application'
          },
          bucket: bucket,
          environment: 'environment'
        };
        var newMeta = {
          key: 'newValue'
        };
        var result;
        before(function () {
          sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve(bucket));
          newBucketPipeline = new BucketPipeline();
          sinon.stub(Context, 'get').returns(Promise.resolve(context));
          return (result = newBucketPipeline.saveMeta(newMeta));
        });
        after(function () {
          Bucket.findOneAsync.restore();
          Context.get.restore();
        });
        it('calls bucket.findOneAsync with correct args', function () {
          return expect(Bucket.findOneAsync.calledWith({
            key: context.bucket.key,
            environment: context.environment,
            application: context.application._id
          })).to.eql(true);
        });
        it('calls Context.get', function () {
          return expect(Context.get.calledOnce).to.eql(true);
        });
        it('sets bucket.meta', function () {
          return expect(result).to.become({
            key1: 'value',
            key: 'newValue'
          });
        });
      });
      describe('with no context.bucket', function () {
        var newBucketPipeline;
        var context = {
          application: {
            _id: 'application'
          },
          environment: 'environment'
        };
        var meta = {
          key: 'newValue'
        };
        var error;
        before(function (done) {
          sinon.stub(Bucket, 'findOneAsync').returns(Promise.resolve());
          newBucketPipeline = new BucketPipeline();
          sinon.stub(Context, 'get').returns(Promise.resolve(context));
          return newBucketPipeline.saveMeta(meta).catch(function (err) {
            error = err;
            done();
          });
        });
        after(function () {
          Bucket.findOneAsync.restore();
          Context.get.restore();
        });
        it('does not call bucket.findOneAsync', function () {
          return expect(Bucket.findOneAsync.called).to.eql(false);
        });
        it('calls Context.get', function () {
          return expect(Context.get.calledOnce).to.eql(true);
        });
        it('rejects', function () {
          expect(error)
            .to.be.instanceOf(Errors.bucket.NotFoundError);
        });
      });
    });
  });
});