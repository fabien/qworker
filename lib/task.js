var support = require('./support');
var extractProperties = support.extractProperties;
var Lifecycle = require('./lifecycle');
var async = require('async');
var path = require('path');
var _ = require('lodash');

// Task metadata:
//
// - id: urlified unique identifier
// - name: descriptive name
// - description: task description
//
// - jobAttributes (array): attibutes to use for notifications
// - job(Start|Progress|Success|Fail)Attributes: same as above

var attributes = ['id', 'name'];

function Task(worker, metadata) {
    this.worker = worker;
    _.extend(this, metadata);
    Lifecycle.attach(this);
};

module.exports = Task;

Task.prototype.notify = function(type) {
    var args = ['Task', this.id, type].concat(_.rest(arguments));
    this.worker.sendNotification.apply(this.worker, args);
};

Task.prototype.log = function() {
    this.notify.apply(this, ['log'].concat(_.toArray(arguments)));
};

Task.prototype.info = function() {
    this.notify.apply(this, ['log:info'].concat(_.toArray(arguments)));
};

Task.prototype.warn = function() {
    this.notify.apply(this, ['log:warn'].concat(_.toArray(arguments)));
};

Task.prototype.error = function() {
    this.notify.apply(this, ['log:error'].concat(_.toArray(arguments)));
};

Task.prototype.fail = function() {
    this.notify.apply(this, ['log:fail'].concat(_.toArray(arguments)));
};

Task.prototype.toJSON = Task.prototype.toBSON = function() {
    return extractProperties(this, attributes);
};

Task.prototype.emit = function(eventName) {
    eventName = 'task:' + eventName;
    this.worker.emit.apply(this.worker, [eventName, this].concat(_.rest(arguments)));
};

Task.prototype.prepareJob = function(job) {
    job.task = this;
    _.extend(job, this.constructor.JobMethods);
};

Task.loadDefinitions = function(paths, callback) {
    support.loadDefinitions(paths, callback);
};

Task.JobMethods = {
    
    notify: function(type) {
        var args = ['Job', String(this.attrs._id), type].concat(_.rest(arguments));
        this.task.worker.sendNotification.apply(this.task.worker, args);
    },
    
    log: function() {
        this.notify.apply(this, ['log'].concat(_.toArray(arguments)));
    },
    
    info: function() {
        this.notify.apply(this, ['log:info'].concat(_.toArray(arguments)));
    },
    
    warn: function() {
        this.notify.apply(this, ['log:warn'].concat(_.toArray(arguments)));
    },
    
    error: function() {
        this.notify.apply(this, ['log:error'].concat(_.toArray(arguments)));
    },
    
    fail: function() {
        this.notify.apply(this, ['log:fail'].concat(_.toArray(arguments)));
    },
    
    progress: function(progress) {
        this.task.worker.onJobProgress(this, progress);
    }
    
};
