var Agenda = require('agenda');
var mubsub = require('mubsub');
var assert = require('assert');
var async = require('async');
var path = require('path');
var util = require('util');
var _ = require('lodash');

var vsprintf = require('sprintf-js').vsprintf;
var EventEmitter = require('events').EventEmitter;
var extractProperties = require('./support').extractProperties;
var Lifecycle = require('./lifecycle');

var Task = require('./task');

var attributes = [
    'id', 'pid', 'taskIds', 'startedAt', 'pingedAt',
    'standalone', 'silent'
];

function Worker(options) {
    options = _.extend({ taskIds: [] }, options);
    options.startedAt = options.startedAt || new Date();
    assert(options.id);
    assert(options.pid);
    assert(_.isDate(options.startedAt));
    assert(_.isArray(options.taskIds));
    
    if (_.isString(options.paths)) {
        options.paths = options.paths.path.split(',')
    } else if (!_.isArray(options.paths)) {
        options.paths = [path.join(process.cwd(), 'tasks')]
    }
    
    this.options = _.defaults(_.omit(options, attributes), this.defaults);
    _.extend(this, _.pick(options, attributes));
    
    this.onExit = _.once(this._onExit.bind(this));
    this._bindEvents();
    this.on('stop', this._bindEvents.bind(this, true));
    
    Lifecycle.attach(this, { types: ['initializer', 'finalizer'] });
};

util.inherits(Worker, EventEmitter);

Worker.Task = Task;

module.exports = Worker;

Worker.prototype.defaults = {
    standalone: false,
    silent: false,
    persistJobs: false,
    bindProcessEvents: true,
    jobAttributes: ['data', 'lastRunAt', 'nextRunAt', 'lastFinishedAt'],
    jobProgressAttributes: []
};

Worker.prototype.connect = function(uri, options, callback) {
    if (this.client) return;
    if (_.isFunction(options)) callback = options, options = {};
    assert(_.isString(uri));
    
    this.tasks = {};
    
    this.jobs = [];
    
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

Worker.prototype.disconnect = function(callback) {
    if (this.client) {
        this.emit('disconnected', this.channel);
        this.channel.publish('worker:disconnected', this);
        this.sendNotification('Worker', this.id, 'disconnected');
        this.client.close();
        delete this.client;
        if (_.isFunction(callback)) callback();
    }
};

Worker.prototype.task = function(name, options, processor) {
    if (!this.agenda) throw new Error('Agenda is not active');
    if (_.isFunction(options)) processor = options, options = {};
    options = _.extend({}, this.options.taskDefaults, {});
    var worker = this;
    var prepareJob = this.prepareJob.bind(this);
    return this.agenda.define(name, options, function(job, done) {
        if (prepareJob(job)) {
            job.task.lifecycle.executeHandlers('observer', { job: job }, function(err) {
                if (err) return done(err);
                if (processor.length === 2) { // async
                    processor(job, done);
                } else {
                    processor.call(null, job);
                    process.nextTick(done);
                }
            });
        } else {
            process.nextTick(done);
        }
    });
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

Worker.prototype.initialize = function(callback) {
    this.emit('connected', this.channel);
    this.channel.publish('worker:connected', this);
    this.sendNotification('Worker', this.id, 'connected');
    
    this.loadTaskDefinitions(function() {
        this.trigger('initialize');
        
        this.channel.on('error', this.error.bind(this, 'error'));
        
        this.channel.subscribe('manager:start', this.onManagerStart.bind(this));
        this.channel.subscribe('manager:stop', this.onManagerStop.bind(this));
        
        this.channel.subscribe('job:cancel', this.onJobCancel.bind(this));
        
        // Worker-specific events (partitioned by process.id)
        this.subscribe('start', this.start.bind(this));
        this.subscribe('stop', this.stop.bind(this));
        this.subscribe('terminate', this.terminate.bind(this));
        
        if (_.isFunction(callback)) callback();
    }.bind(this));
};

Worker.prototype.start = function(options, callback) {
    if (_.isFunction(options)) callback = options, options = null;
    options = _.extend({}, options);
    var self = this;
    if (options.pingInterval) this.options.pingInterval = options.pingInterval;
    if (this.agenda instanceof Agenda) {
        self.trigger('before:restart');
        this.restartAgenda(options, function() {
            self.trigger('restart');
        });
    } else {
        self.trigger('before:start');
        async.series([
            self.setupAgenda.bind(self, options),
            self.lifecycle.executeHandlers.bind(self.lifecycle, 'initializer'),
            self.startAgenda.bind(self)
        ], function(err) {
            if (!err) self.trigger('start');
            callback && callback(err);
        });
    }
};

Worker.prototype.terminate = function(err, callback) {
    if (_.isFunction(err)) callback = err, err = null;
    this.stop(err, function() { this.trigger('terminate'); }.bind(this));
};

Worker.prototype.stop = function(err, callback) {
    if (_.isFunction(err)) callback = err, err = null;
    if (this._terminated) return callback && callback();
    var self = this;
    this._terminated = true;
    
    if (err instanceof Error) {
        this.error = _.omit(err, 'stack');
    } else if (_.isString(err)) {
        this.error = new Error(err);
    }
    
    self.trigger('before:stop');
    
    async.series([
        self.cancelJobs.bind(self),
        self.cleanup.bind(self),
        self.stopAgenda.bind(self),
        self.unloadTasks.bind(self, _.values(self.tasks)),
        self.lifecycle.executeHandlers.bind(self.lifecycle, 'finalizer')
    ], function(err) {
        self.trigger('stop'); // ignore error, stop anyway
        var error = self.error instanceof Error ? self.error : null;
        self.stopPing();
        self.disconnect();
        if (error) self.emit('error', error);
        callback && callback(err || error);
    });
};

Worker.prototype.cleanup = function(callback) {
    if (this.agenda instanceof Agenda && this.agenda._collection) {
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

// Job cancellation

Worker.prototype.cancelJob = function(job, cb) {
    job.task.lifecycle.executeHandlers('terminator', { job: job }, cb);
};

Worker.prototype.cancelJobs = function(callback) {
    async.each(this.jobs, this.cancelJob.bind(this), callback);
};

Worker.prototype.onJobCancel = function(id) {
    var job = _.find(this.jobs, function(job) {
        return String(job.attrs._id) === id;
    });
    if (job) this.cancelJob(job, _.noop);
};

// Manager

Worker.prototype.onManagerStart = function(appId) {
    this.emit('manager:start', appId);
    this.loadTaskDefinitions(function() {
        if (this.agenda) {
            this.agenda.start();
        } else { // retry
            this.trigger('initialize');
        }
    }.bind(this));
};

Worker.prototype.onManagerStop = function(appId) {
    this.emit('manager:stop', appId);
    if (this.agenda && !this.standalone) {
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
    if (this.silent) {
        this.trigger('emit', entry);
    } else {
        this.trigger('notification', entry);
    }
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

// Agenda

Worker.prototype.setupAgenda = function(options, callback) {
    if (this.agenda instanceof Agenda) return; // singleton
    options = _.extend({}, this.options.agenda, options);
    
    var db = options.db || this.options.uri;
    var collectionName = options.collection || 'Job';
    
    var agenda = this.agenda = new Agenda(_.pick(options, 'processEvery'));
    
    var onReady = _.once(function(err) {
        if (err) return callback(err, agenda);
        this.loadTasks(options.tasks, function(err, tasks) {
            if (err) return callback(err, agenda);
            var indexed = _.indexBy(tasks, 'id');
            this.tasks = _.pick(this.tasks, _.keys(indexed));
            this.tasks = _.merge(this.tasks, indexed);
            this.db = agenda._mdb;
            if (_.isFunction(callback)) callback();
        }.bind(this));
    }.bind(this));
    
    agenda.once('ready', onReady.bind(null, null));
    agenda.once('error', onReady.bind(null));
    
    agenda.on('error', this.onAgendaError.bind(this));
    
    agenda.on('start', this.onJobStart.bind(this));
    agenda.on('success', this.onJobSuccess.bind(this));
    agenda.on('fail', this.onJobFail.bind(this));
    
    agenda.name(this.id);
    agenda.database(options.db, collectionName);
};

Worker.prototype.startAgenda = function(callback) {
    callback = callback || _.noop;
    if (this.agenda instanceof Agenda || this.options.disabled) {
        this.unlockTasks(function() {
            this.emit('agenda:start', this.agenda);
            if (this.agenda) this.agenda.start();
        }.bind(this));
    } else {
        callback();
    }
};

Worker.prototype.stopAgenda = function(callback) {
    callback = callback || _.noop;
    if (this.agenda instanceof Agenda && this.agenda._collection) {
        this.emit('agenda:stop', this.agenda);
        this.agenda.stop(function(err) {
            delete this.agenda;
            callback(err);
        }.bind(this));
    } else {
        callback();
    }
};

Worker.prototype.restartAgenda = function(options, callback) {
    if (!this.agenda) return;
    if (_.isFunction(options)) callback = options, options = null;
    options = _.extend({}, this.options.agenda, options);
    this.emit('agenda:restart', this.agenda);
    this.stopAgenda(function() {
        this.setupAgenda(options, function(err) {
            if (err) return callback(err);
            this.startAgenda(callback);
        }.bind(this));
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
    this.jobs.push(job);
    this.onJobEvent('start', job);
};

Worker.prototype.onJobSuccess = function(job) {
    this.onJobEvent('success', job);
    this.onJobComplete(job);
};

Worker.prototype.onJobFail = function(error, job) {
    var task = this.getTask(job.attrs.name);
    var data = this.extractJobAttributes('fail', job, {
        job: { error: errorToJSON(error) }
    });
    if (error instanceof Error) data.msg = error.message;
    this.emit('job:fail', error, job, task, data);
    this.sendNotification('Job', String(job.attrs._id), 'fail', data);
    this.onJobComplete(job);
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

Worker.prototype.onJobComplete = function(job) {
    _.pull(this.jobs, job);
    if (this.shouldPersistJob(job)) {
        if (job.attrs.failedAt || job.attrs.failReason) {
            job.attrs.failedAt = job.attrs.failReason = null;
            job.save();
        }
    } else {
        job.remove(_.noop);
    }
};

Worker.prototype.onJobEvent = function(eventName, job, data, attrs) {
    var task = this.getTask(job.attrs.name);
    data = this.extractJobAttributes(eventName, job, data, attrs);
    this.emit('job:' + eventName, job, task, data);
    this.sendNotification('Job', String(job.attrs._id), eventName, data);
};

Worker.prototype.shouldPersistJob = function(job) {
    var task = this.getTask(job.attrs.name);
    if (!_.isEmpty(job.attrs.repeatInterval)) {
        return true; // always persist recurring jobs
    } else if (task && _.isEmpty(job.attrs.repeatInterval)) {
        return task.persistJobs || this.options.persistJobs;
    } else {
        return false;
    }
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

Worker.prototype.loadTaskDefinitions = function(callback) {
    Task.loadDefinitions(this.options.paths, function(err, tasks) {
        if (err) this.emit('error', err);
        if (!_.isEmpty(tasks)) this.tasks = _.indexBy(tasks, 'id');
        if (!_.isEmpty(tasks)) this.channel.publish('define:tasks', tasks);
        callback(err, tasks);
    }.bind(this));
};

Worker.prototype.loadTasks = function(tasks, callback) {
    async.map(tasks || [], function(meta, next) {
        var task = new Task(this, meta);
        this.emit('task:load', task);
        require(meta.definition)(this, task);
        task.lifecycle.executeHandlersOnce('initializer', function(err) {
            next(err, task);
        });
    }.bind(this), callback);
};

Worker.prototype.unloadTasks = function(tasks, callback) {
    async.map(tasks || [], function(task, next) {
        this.emit('task:unload', task);
        task.lifecycle.executeHandlersOnce('finalizer', function(err) {
            next(err, task);
        });
    }.bind(this), callback);
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
    if (!this.options.bindProcessEvents) return;
    var events = ['exit', 'uncaughtException', 'SIGTERM', 'SIGINT'];
    _.each(events, function(eventName) {
        if (remove) {
            process.removeListener(eventName, this.onExit);
        } else {
            process.addListener(eventName, this.onExit);
        }
    }.bind(this));
    process.on('message', function(msg) {
        if (msg == 'shutdown') this.onExit();
    }.bind(this));
};

Worker.prototype._onExit = function(err) {
    if (err) console.log(err.stack);
    this.terminate(err, function() { process.exit(err ? 1 : 0); });
};

// Helpers

function errorToJSON(error) {
    var json = {};
    Object.getOwnPropertyNames(error).forEach(function(key) {
        if (key !== 'stack') json[key] = error[key];
    });
    return json;
};
