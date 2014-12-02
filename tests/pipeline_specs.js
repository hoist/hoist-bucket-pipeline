'use strict';
var bucketPipeline = require('../lib/pipeline');
var chai = require('chai');
chai.use(require('chai-as-promised'));
var expect = chai.expect;
var sinon = require('sinon');
var BBPromise = require('bluebird');

describe('bucketPipeline', function () {
  describe('with key and meta', function () {
    var newbucketPipeline;
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
      newbucketPipeline = new bucketPipeline();
      bucket.saveAsync = sinon.stub().returns(BBPromise.resolve(bucket));
      sinon.stub(newbucketPipeline, 'createBucket').returns(bucket);
      sinon.stub(newbucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
      return  (result = newbucketPipeline.add('pthfut76-7ehfgdt23sw', {testMeta: 'test'}));
    });
    after(function () {
      newbucketPipeline.Context.get.restore();
      newbucketPipeline.createBucket.restore();
    });
    it('calls hoist-context', function () {
      return expect(newbucketPipeline.Context.get.calledOnce).to.eql(true);
    });
    it('creates a new bucket with correct args', function () {
      return expect(newbucketPipeline.createBucket.calledWith(options)).to.eql(true);
    });
    
    it('saves the new bucket', function () {
      return expect(bucket.saveAsync.calledOnce).to.eql(true);
    });
    it('returns the new bucket', function () {
       expect(result).to.become(bucket);
    });
  });
  describe('with no key and meta', function () {
    var newbucketPipeline;
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
      newbucketPipeline = new bucketPipeline();
      bucket.saveAsync = sinon.stub().returns(BBPromise.resolve(bucket));
      sinon.stub(newbucketPipeline, 'createBucket').returns(bucket);
      sinon.stub(newbucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
      return  (result = newbucketPipeline.add());
    });
    after(function () {
      newbucketPipeline.Context.get.restore();
      newbucketPipeline.createBucket.restore();
    });
    it('calls hoist-context', function () {
      return expect(newbucketPipeline.Context.get.calledOnce).to.eql(true);
    });
    it('creates a new bucket with correct args', function () {
      return expect(newbucketPipeline.createBucket.calledWith(options)).to.eql(true);
    });
    
    it('saves the new bucket', function () {
      return expect(bucket.saveAsync.calledOnce).to.eql(true);
    });
    it('returns the new bucket', function () {
       expect(result).to.become(bucket);
    });
  });
});