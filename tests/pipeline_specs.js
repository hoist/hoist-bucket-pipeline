// 'use strict';
// var bucketPipeline = require('../lib/pipeline');
// var expect = require('chai').expect;
// var sinon = require('sinon');
// var BBPromise = require('bluebird');

// describe('bucketPipeline', function () {
//   var newbucketPipeline;
//   var contextGetStub;
//   var context = {
//     application: 'application',
//     environment: 'environment'
//   };
//   before(function () {
    
//     newbucketPipeline = new bucketPipeline();
//     contextGetStub = sinon.stub(newbucketPipeline.Context, 'get').returns(BBPromise.resolve(context));
//     newbucketPipeline.add('pthfut76-7ehfgdt23sw', {testMeta: 'test'});
//   });
//   after(function () {
//     newbucketPipeline.Context.get.restore();
//   });
//   it('calls hoist-context', function () {
//     // return expect(contextGetStub.calledOnce).to.eql(true);
//   });
    
// });