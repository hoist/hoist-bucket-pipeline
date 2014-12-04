'use strict';
var bucketPipeline = require('../lib/pipeline').Pipeline;
var chai = require('chai');
chai.use(require('chai-as-promised'));
var expect = chai.expect;
var sinon = require('sinon');
var BBPromise = require('bluebird');
var Bucket = require('hoist-model').Bucket;
var HoistErrors = require('hoist-errors');

describe('bucketPipeline', function () {
  describe('.add', function () {
    describe('with no arguments', function () {
      var bucket = {};
      var newBucketPipeline;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        return newBucketPipeline.add();
      });
      after(function () {
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith(null, null)).to.be.true;
      });
    });
    describe('with valid key argument', function () {
      var newBucketPipeline;
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var context = {
        application: {
          _id:'application'
        },
        environment: 'environment'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.add(fakeKey);
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith(fakeKey, null)).to.eql(true);
      });
    });
    describe('with a duplicate key argument', function () {
      var bucket = {};
      var newBucketPipeline;
      var fakeKey = 'fake key';
      var error;
      var context = {
        application: {
          _id:'application'
        },
        environment: 'environment'
      };
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        newBucketPipeline.add(fakeKey).catch(function(err){
          error = err;
          done();
        });
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('rejects', function () {
        expect(error)
          .to.be.instanceOf(HoistErrors.bucket.InvalidError)
          .and.have.property('message', 'A bucket with key "'+fakeKey+'" already exists');
      });
      
    });
    describe('with valid meta argument', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakeKey: 'fake data'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        return newBucketPipeline.add(fakeMeta);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith( null, fakeMeta)).to.eql(true);
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
          _id:'application'
        },
        environment: 'environment'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.add(fakeKey, fakeMeta);
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith(fakeKey, fakeMeta)).to.eql(true);
      });
    });

    describe('with valid key and meta arguments, with meta first', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakeKey: 'fake data'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var context = {
        application: {
          _id:'application'
        },
        environment: 'environment'
      };
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.add( fakeMeta, fakeKey);
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith(fakeKey, fakeMeta)).to.eql(true);
      });
    });
  });

  describe('.addHelper', function () {
    describe('with key and meta', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id:'application'
        },
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        key: 'pthfut76-7ehfgdt23sw',
        meta: {testMeta: 'test'},
        application: 'application',
        environment: 'environment'
      };
      before(function () {
        bucket = {};
        newBucketPipeline = new bucketPipeline();
        bucket.saveAsync = sinon.stub().returns(BBPromise.resolve(bucket));
        sinon.stub(newBucketPipeline, 'createBucket').returns(bucket);
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return  (result = newBucketPipeline.addHelper('pthfut76-7ehfgdt23sw', {testMeta: 'test'}));
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        newBucketPipeline.createBucket.restore();
      });
      it('calls hoist-context', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
      });
      it('creates a new bucket with correct args', function () {
        return expect(newBucketPipeline.createBucket.calledWith(options)).to.eql(true);
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
          _id:'application'
        },
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        application: 'application',
        environment: 'environment'
      };
      before(function () {
        bucket = {};
        newBucketPipeline = new bucketPipeline();
        bucket.saveAsync = sinon.stub().returns(BBPromise.resolve(bucket));
        sinon.stub(newBucketPipeline, 'createBucket').returns(bucket);
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return  (result = newBucketPipeline.addHelper());
      });
      after(function () {
        newBucketPipeline.Context.get.restore();
        newBucketPipeline.createBucket.restore();
      });
      it('calls hoist-context', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
      });
      it('creates a new bucket with correct args', function () {
        return expect(newBucketPipeline.createBucket.calledWith(options)).to.eql(true);
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
          _id:'application'
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
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.set(fakeKey);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeKey, environment: context.environment, application: context.application._id})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
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
          _id:'application'
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
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(null));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        sinon.stub(newBucketPipeline, 'add').returns(BBPromise.resolve(bucket));
        return newBucketPipeline.set(fakeKey, true);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
        newBucketPipeline.add.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeKey, environment: context.environment, application: context.application._id})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
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
          _id:'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(null));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        newBucketPipeline.set(fakeKey).catch(function(err){
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('rejects', function () {
        return expect(error)
        .to.be.instanceOf(HoistErrors.bucket.NotFoundError);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.called).to.eql(true);
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeKey, environment: context.environment, application: context.application._id})).to.eql(true);
      });
    });
  });

  describe('.get', function () {
    describe('with a existing bucket key', function () {
      var newBucketPipeline;
      var context = {
        application: {
          _id:'application'
        },
        environment: 'environment'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var bucket = {
        key: fakeKey
      };
      var result;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return (result = newBucketPipeline.get(fakeKey));
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeKey, environment: context.environment, application: context.application._id})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
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
          _id:'application'
        },
        environment: 'environment'
      };
      var error;
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(null));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.get(fakeKey).catch(function (err) {
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeKey, environment: context.environment, application: context.application._id})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
      });
      it('throws bucket not found error', function () {
        return expect(error)
        .to.be.instanceOf(HoistErrors.bucket.NotFoundError);
      });
    });
    describe('with no key', function () {
      var newBucketPipeline;
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      var context = {
        application: {
          _id:'application'
        },
        bucket: {
          key: fakeKey
        },
        environment: 'environment'
      };
      var bucket = {
        key: fakeKey
      };
      var result;
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return (result = newBucketPipeline.get());
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: context.bucket.key, environment: context.environment, application: context.application._id})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
      });
      it('returns bucket', function () {
        return expect(result).to.become(bucket);
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
      sinon.stub(Bucket, 'findAsync').returns(BBPromise.resolve([]));
      newBucketPipeline = new bucketPipeline();
      sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
      return (result = newBucketPipeline.getAll());
    });
    after(function () {
      Bucket.findAsync.restore();
      newBucketPipeline.Context.get.restore();
    });
    it('calls bucket.findOneAsync with correct args', function () {
      return expect(Bucket.findAsync.calledWith({
        environment: context.environment,
        application: context.application._id
      })).to.eql(true);
    });
    it('calls Context.get', function () {
      return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
    });
    it('returns bucket', function () {
      return expect(result).to.become([]);
    });
  });
  describe('.each', function () {
    var newBucketPipeline;
    var buckets = [{
      key: 'key1'
    },{
      key: 'key2'
    }];
    var result = [];
    before(function () {
      newBucketPipeline = new bucketPipeline();
      sinon.stub(newBucketPipeline, 'getAll').returns(BBPromise.resolve(buckets));
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
});