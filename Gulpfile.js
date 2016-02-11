'use strict';
var gulp = require('gulp');
var requireDir = require('require-dir');
var runSequence = require('run-sequence');
require('git-guppy')(gulp);
var helpers = require('./gulp/helpers');

requireDir('./gulp/tasks', {
  recurse: true
});
gulp.task('test', function (cb) {
  runSequence('clean-coverage', 'transpile', ['eslint-build', 'mocha-server'], () => {
    cb(helpers.getError());
  });
});
gulp.task('default', function () {
  return gulp.start('test');
});

gulp.task('post-commit', ['test'], () => {

});

gulp.task('pre-commit', ['transpile'], function (cb) {
  runSequence('clean', ['transpile', 'esdoc'], cb);
});
