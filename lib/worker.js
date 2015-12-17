var Agenda = require('agenda');
var mubsub = require('mubsub');
var assert = require('assert');
var util = require('util');
var _ = require('lodash');

var vsprintf = require('sprintf-js').vsprintf;
var EventEmitter = require('events').EventEmitter;
var extractProperties = require('./support').extractProperties;

var Task = require('./task');

var attributes = ['id', 'pid', 'taskIds', 'startedAt', 'pingedAt'];

function Worker(options) {
    options = _.extend({ taskIds: [] }, options);
    options.startedAt = options.startedAt || new Date();
    assert(options.id);
    assert(options.pid);
    assert(_.isDate(options.startedAt));
    assert(_.isArray(options.taskIds));
    
    this.options = _.defaults(_.omit(options, attributes), this.defaults);
    _.extend(this, _.pick(options, attributes));
    
    this.onExit = _.once(this._onExit.bind(this));
    this._bindEvents();
    this.on('stop', this._bindEvents.bind(this, true));
};

util.inherits(Worker, EventEmitter);

Worker.Task = Task;

module.exports = Worker;

Worker.prototype.defaults = {
    standalone: false,
    jobAttributes: ['data', 'lastRunAt', 'nextRunAt', 'lastFinishedAt'],
    jobProgressAttributes: []
};

Worker.prototype.connect = function(uri, options, callback) {
    if (this.client) return;
    if (_.isFunction(options)) callback = options, options = {};
    assert(_.isString(uri));
    
    this.tasks = {};
    
    var channelName = this.options.channelName || 'mubsub';
    var channelOptions = _.extend({}, this.options.channelOptions);
    this.options.uri = uri;
    this.client = mubsub(uri, options);
    this.channel = this.client.channel(channelName, channelOptions);
    
    this.channel.once('ready', this.initialize.bind(this));
    
    this.on('agenda:start', this.onAgendaStart.bind(this));
    this.on('agenda:stop', this.onAgendaStop.bind(this));
    
    this.on('agenda:start', this.startPing.bind(this));
    this.on('agenda:stop', this.stopPing.bind(this));
    
    var onReady = _.once(_.isFunction(callback) ? callback : _.noop);
    this.channel.once('ready', onReady.bind(null, null));
    this.channel.once('error', onReady.bind(null));
};

Worker.prototype.disconnect = function() {
    if (this.client) {
        this.sendNotification('Worker', this.id, 'disconnected');
        this.emit('disconnected');
        this.client.close();
    }
};

Worker.prototype.task = function(name, options, processor) {
    if (!this.agenda) throw new Error('Agenda is not active');
    if (_.isFunction(options)) processor = options, options = {};
    options = _.extend({}, this.options.taskDefaults, {});
    var prepareJob = this.prepareJob.bind(this);
    if (processor.length === 2) { // async
        return this.agenda.define(name, options, function(job, done) {
            if (prepareJob(job)) {
                processor(job, done);
            } else {
                done(new Error('Invalid task: ' + job.attrs.name));
            }
        });
    } else {
        return this.agenda.define(name, options, function(job) {
            if (prepareJob(job)) processor.call(null, job);
        });
    }
};

Worker.prototype.getTask = function(name) {
    return this.tasks[name];
};

Worker.prototype.scheduleTask = function(when, name, options, callback) {
    if (!this.channel) throw new Error('Worker is not connected');
    if (_.isFunction(options)) callback = options, callback = {};
    options = _.extend({}, options);
    if (when === 'now' || when === true) when = new Date();
    this.channel.publish('schedule:task', {
        when: when, name: name, options: options
    }, callback);
};

Worker.prototype.prepareJob = function(job) {
    var task = this.getTask(job.attrs.name);
    if (task) task.prepareJob(job);
    return job.task instanceof Task;
};

// Lifecycle

Worker.prototype.initialize = function() {
    this.emit('connected', this.channel);
    this.sendNotification('Worker', this.id, 'connected');
    
    this.trigger('initialize');
    
    this.channel.on('error', this.error.bind(this, 'error'));
    
    this.channel.subscribe('manager:start', this.onManagerStart.bind(this));
    this.channel.subscribe('manager:stop', this.onManagerStop.bind(this));
    
    // Worker-specific events (partitioned by process.id)
    this.subscribe('start', this.start.bind(this));
    this.subscribe('stop', this.stop.bind(this));
    this.subscribe('terminate', this.terminate.bind(this));
};

Worker.prototype.start = function(options) {
    options = _.extend({}, options);
    if (options.pingInterval) this.options.pingInterval = options.pingInterval;
    if (this.agenda instanceof Agenda) {
        this.restartAgenda(options);
    } else {
        this.startAgenda(options, this.trigger.bind(this, 'start', null));
    }
};

Worker.prototype.stop = function(err, callback) {
    if (_.isFunction(err)) callback = err, err = null;
    this.terminate(err, function() { this.trigger('stop'); }.bind(this));
};

Worker.prototype.terminate = function(err, callback) {
    if (_.isFunction(err)) callback = err, err = null;
    if (this._terminated) return callback && callback();
    this._terminated = true;
    if (err instanceof Error) {
        this.error = _.omit(err, 'stack');
    } else if (_.isString(err)) {
        this.error = new Error(err);
    }
    if (this.error instanceof Error) this.emit('error', this.error);
    this.cleanup(function() {
        this.stopAgenda(function() {
            if (_.isFunction(callback)) callback(err); // important: first
            this.stopPing();
            this.disconnect();
        }.bind(this));
    }.bind(this));
};

Worker.prototype.cleanup = function(callback) {
    if (this.agenda instanceof Agenda) {
        var query = {
            lastModifiedBy: this.id,
            $and: [
                { $or: [{ lastFinishedAt: { $exists: false } }, { lastFinishedAt: null }] },
                { $or: [{ repeatInterval: { $exists: false } }, { repeatInterval: null }] }
            ]
        };
        this.agenda.cancel(query, callback);
    } else if (_.isFunction(callback)) {
        callback(null, 0);
    }
};

// Manager

Worker.prototype.onManagerStart = function(appId) {
    this.emit('manager:start', appId);
    if (this.agenda) {
        this.agenda.start();
    } else { // retry
        this.trigger('initialize');
    }
};

Worker.prototype.onManagerStop = function(appId) {
    this.emit('manager:stop', appId);
    if (this.agenda && !this.options.standalone) {
        this.emit('pause', appId);
        this.agenda.stop();
    }
};

// Pubsub

Worker.prototype.trigger = function(eventName, data, callback) {
    if (_.isFunction(data)) callback = data, data = null;
    data = (_.isNull(data) || arguments.length === 1) ? this : data;
    if (this.channel) {
        this.channel.publish('worker:' + eventName, data, function() {
            this.emit(eventName, data); // local events
            if (_.isFunction(callback)) callback();
        }.bind(this));
    } else {
        this.emit(eventName, data); // local events
        if (_.isFunction(callback)) process.nextTick(callback);
    }
};

Worker.prototype.publish = function(eventName, data, callback) {
    if (!this.channel) throw new Error('Worker is not connected');
    this.channel.publish(this.pid + '/' + eventName, data, callback);
};

Worker.prototype.subscribe = function(eventName, callback) {
    if (!this.channel) throw new Error('Worker is not connected');
    this.channel.subscribe(this.pid + '/' + eventName, callback);
};

// Notification and logging

Worker.prototype.sendNotification = function(sourceType, sourceId, type, msg) {
    var entry = { workerId: this.id, sourceType: sourceType, sourceId: sourceId };
    entry.type = type;
    entry.triggeredAt = new Date();
    if (_.isString(msg)) {
        entry.msg = vsprintf(msg, _.toArray(arguments).slice(4));
    } else if (_.isObject(msg)) {
        _.extend(entry, msg);
    }
    this.trigger('notification', entry);
};

Worker.prototype.notify = function(type) {
    var args = ['Worker', this.id, type].concat(_.rest(arguments));
    this.sendNotification.apply(this, args);
};

Worker.prototype.log = function() {
    this.notify.apply(this, ['log'].concat(_.toArray(arguments)));
};

Worker.prototype.info = function() {
    this.notify.apply(this, ['log:info'].concat(_.toArray(arguments)));
};

Worker.prototype.warn = function() {
    this.notify.apply(this, ['log:warn'].concat(_.toArray(arguments)));
};

Worker.prototype.error = function() {
    this.notify.apply(this, ['log:error'].concat(_.toArray(arguments)));
};

Worker.prototype.fail = function() {
    this.notify.apply(this, ['log:fail'].concat(_.toArray(arguments)));
};

// Agenda

Worker.prototype.startAgenda = function(options, callback) {
    if (this.agenda instanceof Agenda) return; // singleton
    options = _.extend({}, this.options.agenda, options);
    
    var db = options.db || this.options.uri;
    var collectionName = options.collection || 'QueuedJob';
    
    var agenda = this.agenda = new Agenda();
    
    agenda.name(this.id);
    agenda.database(options.db, collectionName);
    
    var onReady = _.once(_.isFunction(callback) ? callback : _.noop);
    agenda.once('ready', onReady.bind(null, null));
    agenda.once('error', onReady.bind(null));
    
    agenda.on('ready', this.onAgendaReady.bind(this));
    agenda.on('error', this.onAgendaError.bind(this));
    
    agenda.on('start', this.onJobStart.bind(this));
    agenda.on('success', this.onJobSuccess.bind(this));
    agenda.on('fail', this.onJobFail.bind(this));
    
    this.tasks = _.indexBy(this.loadTasks(options.tasks), 'id');
};

Worker.prototype.stopAgenda = function(callback) {
    if (this.agenda instanceof Agenda) {
        this.emit('agenda:stop', this.agenda);
        var ref = this.agenda;
        delete this.agenda;
        ref.stop(callback);
    } else if (callback) {
        callback();
    }
};

Worker.prototype.restartAgenda = function(options, callback) {
    if (!this.agenda) return;
    options = _.extend({}, this.options.agenda, options);
    this.emit('agenda:restart', this.agenda);
    this.stopAgenda(this.startAgenda.bind(this, options, callback));
};

Worker.prototype.onAgendaReady = function() {
    this.emit('agenda:start', this.agenda);
    this.unlockTasks(function() {
        this.agenda.start();
    }.bind(this));
};

Worker.prototype.onAgendaStart = function() {
    this.sendNotification('Worker', this.id, 'start', { taskIds: this.taskIds || [] });
};

Worker.prototype.onAgendaStop = function() {
    this.sendNotification('Worker', this.id, 'stop', { taskIds: this.taskIds || [] });
};

Worker.prototype.onAgendaError = function(error) {
    this.emit('agenda:error', this.agenda, error);
};

// Jobs

Worker.prototype.onJobStart = function(job) {
    this.onJobEvent('start', job);
};

Worker.prototype.onJobSuccess = function(job) {
    this.onJobEvent('success', job);
    if (_.isEmpty(job.attrs.repeatInterval)) {
        job.remove(_.noop);
    } else if (job.attrs.failedAt || job.attrs.failReason) {
        job.attrs.failedAt = job.attrs.failReason = null;
        job.save();
    }
};

Worker.prototype.onJobFail = function(error, job) {
    var task = this.getTask(job.attrs.name);
    var data = this.extractJobAttributes('fail', job, {
        job: { error: errorToJSON(error) }
    });
    this.emit('job:fail', error, job, task, data);
    this.sendNotification('Job', String(job.attrs._id), 'fail', data);
    if (_.isEmpty(job.attrs.repeatInterval)) job.remove(_.noop);
};

Worker.prototype.onJobProgress = function(job, progress) {
    progress = _.isNumber(progress) ? progress : 0;
    progress = Math.max(0, Math.min(progress, 100));
    var task = this.getTask(job.attrs.name);
    var data = this.extractJobAttributes('progress', job, {
        job: { progress: progress }
    });
    this.emit('job:progress', progress, job, task, data);
    this.sendNotification('Job', String(job.attrs._id), 'progress', data);
};

Worker.prototype.onJobEvent = function(eventName, job, data, attrs) {
    var task = this.getTask(job.attrs.name);
    data = this.extractJobAttributes(eventName, job, data, attrs);
    this.emit('job:' + eventName, job, task, data);
    this.sendNotification('Job', String(job.attrs._id), eventName, data);
};

Worker.prototype.extractJobAttributes = function(eventName, job, data, attrs) {
    var task = this.getTask(job.attrs.name);
    var type = _.capitalize(_.camelCase(eventName.replace(':', '-')));
    type = 'job' + type + 'Attributes';
    attrs = attrs || this.options[type] || this.options.jobAttributes;
    var jobAttributes = task[type] || task.jobAttributes;
    if (!_.isEmpty(attrs) && task && _.isArray(jobAttributes)) {
        attrs = jobAttributes;
    }
    var jobAttrs = extractProperties(job.attrs, attrs);
    if (_.isNull(jobAttrs.data)) jobAttrs.data = {};
    return _.merge({ taskId: job.attrs.name, job: jobAttrs }, data);
};

// Tasks

Worker.prototype.loadTasks = function(tasks) {
    return _.map(tasks || [], function(meta) {
        var task = new Task(this, meta);
        this.emit('task:load', task);
        require(meta.definition)(this, task);
        return task;
    }.bind(this));
};

Worker.prototype.unlockTasks = function(callback) {
    if (!this.agenda) return callback && callback();
    this.agenda._collection.update({
        name: { $in: this.taskIds || [] },
        lockedAt : { $exists : true } 
    }, { $set : { lockedAt : null } }, { w: 1, multi: true }, callback);
};

// Ping

Worker.prototype.ping = function() {
    this.trigger('ping');
};

Worker.prototype.startPing = function() {
    this.stopPing();
    var interval = this.options.pingInterval || 5000;
    this._pingInterval = setInterval(this.ping.bind(this), interval);
};

Worker.prototype.stopPing = function() {
    if (this._pingInterval) clearInterval(this._pingInterval);
};

// Serialization

Worker.prototype.toJSON = Worker.prototype.toBSON = function() {
    return extractProperties(this, attributes);
};

// Private

Worker.prototype._bindEvents = function(remove) {
    _.each(['exit', 'uncaughtException', 'SIGTERM', 'SIGINT'], function(eventName) {
        if (remove) {
            process.removeListener(eventName, this.onExit);
        } else {
            process.addListener(eventName, this.onExit);
        }
    }.bind(this));
};

Worker.prototype._onExit = function(err) {
    if (err) console.log(err.stack);
    this.stop(err, function() { process.exit(err ? 1 : 0); });
};

// Helpers

function errorToJSON(error) {
    var json = {};
    Object.getOwnPropertyNames(error).forEach(function(key) {
        if (key !== 'stack') json[key] = error[key];
    });
    return json;
};
