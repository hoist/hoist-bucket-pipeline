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
    describe(' with no arguments', function () {
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
    describe(' with valid key argument', function () {
      var newBucketPipeline;
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        return newBucketPipeline.add(fakeKey);
      });
      after(function () {
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
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(bucket));
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        newBucketPipeline.add(fakeKey).catch(function(err){
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('rejects', function () {
        expect(error)
          .to.be.instanceOf(HoistErrors.bucket.InvalidError)
          .and.have.property('message', 'A bucket with key "'+fakeKey+'" already exists');
      });
      
    });
    describe(' with valid meta argument', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakekey: 'fake data'
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
    });
    describe('with valid key and meta arguments, with key first', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakekey: 'fake data'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        return newBucketPipeline.add(fakeKey, fakeMeta);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        bucketPipeline.prototype.addHelper.restore();
      });
      it('called with correct args', function () {
        return expect(bucketPipeline.prototype.addHelper.calledWith(fakeKey, fakeMeta)).to.eql(true);
      });
    });

    describe(' with valid key and meta arguments, with meta first', function () {
      var newBucketPipeline;
      var fakeMeta = {
        fakekey: 'fake data'
      };
      var fakeKey = '2hgjfkitl98-6_hftgh4';
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve());
        sinon.stub(bucketPipeline.prototype, 'addHelper').returns(BBPromise.resolve());
        newBucketPipeline = new bucketPipeline();
        return newBucketPipeline.add( fakeMeta, fakeKey);
      });
      after(function () {
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
        application: 'application',
        environment: 'environment'
      };
      var bucket;
      var result;
      var options = {
        _id: 'pthfut76-7ehfgdt23sw',
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
        application: 'application',
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
  
  describe('.setCurrent', function () {
    describe('with a valid bucket key', function () {
      var newBucketPipeline;
      var context = {
        application: 'application',
        environment: 'environment'
      };
      var fakeId = '2hgjfkitl98-6_hftgh4';
      before(function () {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve({}));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        return newBucketPipeline.setCurrent(fakeId);
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeId, environment: context.environment, application: context.application})).to.eql(true);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.calledOnce).to.eql(true);
      });
      it('sets context.bucket to the bucket key', function () {
        return expect(context.bucket).to.eql(fakeId);
      });
    });
    describe('with an invalid bucket key', function () {
      var newBucketPipeline;
      var error;
      var context = {
        application: 'application',
        environment: 'environment'
      };
      var fakeId = '2hgjfkitl98-6_hftgh4';
      before(function (done) {
        sinon.stub(Bucket, 'findOneAsync').returns(BBPromise.resolve(null));
        newBucketPipeline = new bucketPipeline();
        sinon.stub(newBucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
        newBucketPipeline.setCurrent(fakeId).catch(function(err){
          error = err;
          done();
        });
      });
      after(function () {
        Bucket.findOneAsync.restore();
        newBucketPipeline.Context.get.restore();
      });
      it('rejects', function () {
        expect(error)
        .to.be.instanceOf(HoistErrors.bucket.NotFoundError);
      });
      it('calls Context.get', function () {
        return expect(newBucketPipeline.Context.get.called).to.eql(true);
      });
      it('calls bucket.findOneAsync with correct args', function () {
        return expect(Bucket.findOneAsync.calledWith({key: fakeId, environment: context.environment, application: context.application})).to.eql(true);
      });
    });
  });

});