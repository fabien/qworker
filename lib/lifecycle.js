var EventEmitter = require('events').EventEmitter;
var async = require('async');
var util = require('util');
var _ = require('lodash');

var defaultTypes = ['initializer', 'finalizer', 'observer'];

function Lifecycle(instance, options) {
    this.options = _.extend({}, options);
    this.options.types = [].concat(this.options.types || defaultTypes);
    this.options.types = _.intersection(this.options.types, defaultTypes);
    this.instance = instance;
    this.instance.lifecycle = this;
    this.reset();
    _.each(this.options.types, function(type) {
        var fn = this.registerHandler.bind(this, type);
        this.instance[type] = fn;
    }.bind(this));
};

util.inherits(Lifecycle, EventEmitter);

Lifecycle.attach = function(instance, options) {
    if (instance.lifecycle instanceof Lifecycle) {
        instance.lifecycle.reset();
    } else {
        new Lifecycle(instance, options);
    }
};

Lifecycle.prototype.getHandler = function(type, name) {
    var handlers = this.handlers[type];
    if (!_.isArray(handlers)) return;
    return _.find(handlers, { name: name });
};

Lifecycle.prototype.executeHandlers = function(type, context, callback) {
    if (_.isFunction(context)) callback = context, context = {};
    var handlers = this.handlers[type] || [];
    async.eachSeries(handlers, this._executeHandler.bind(this, type, context), callback);
};

Lifecycle.prototype.executeHandlersOnce = function(type, callback) {
    this.executeHandlers(type, function(err) {
        this.resetHandlers(type); // regardless of any errors
        if (_.isFunction(callback)) callback(err);
    }.bind(this));
};

Lifecycle.prototype.executeHandler = function(type, name, context, callback) {
    if (_.isFunction(context)) callback = context, context = {};
    var handler = this.getHandler(type, name);
    if (handler) return this._executeHandler(type, context, handler, callback);
};

Lifecycle.prototype.registerHandler = function(type, name, options, handlerFn) {
    var handlers = this.handlers[type];
    if (!_.isArray(handlers)) throw new Error('Invalid lifecycle event: ' + type);
    if (_.isFunction(options)) handlerFn = options, options = null;
    if (!_.isFunction(handlerFn)) throw new Error('Invalid lifecycle handler fn for ' + type + ': ' + name);
    if (this.getHandler(type, name)) return; // already defined - silently ignore
    var item = { name: name, options: options = _.extend({}, options), fn: handlerFn };
    this.trigger('register', type, item);
    handlers.push(item);
};

Lifecycle.prototype.unregisterHandler = function(type, name) {
    var handlers = this.handlers[type];
    if (!_.isArray(handlers)) return;
    var item = _.find(handlers, { name: name });
    if (item) this.trigger('unregister', type, item);
    if (item) return _.pull(handlers, item);
};

Lifecycle.prototype.resetHandlers = function(type) {
    var handlers = this.handlers[type];
    if (!_.isArray(handlers)) return;
    this.handlers[type] = [];
};

Lifecycle.prototype.reset = function() {
    var handlers = this.handlers = {};
    _.each(this.options.types, function(type) { handlers[type] = []; });
    this.trigger('reset');
};

Lifecycle.prototype._executeHandler = function(type, context, handler, callback) {
    if (!handler || !_.isFunction(handler.fn)) {
        return callback && callback(new Error('lifecycle handler fn'));
    }
    var options = _.extend({}, handler.options);
    var ctx = _.extend({ 
        type: type, name: handler.name,
        instance: this.instance, options: options
    }, context);
    if (handler.fn.length === 1) { // async
        handler.fn.call(ctx, function(err, result) {
            this._executedHandler(type, ctx, result, err);
            if (_.isFunction(callback)) callback(err, result, ctx);
        }.bind(this));
    } else {
        var result = handler.fn.call(ctx);
        this._executedHandler(type, ctx, result);
        if (_.isFunction(callback)) {
            process.nextTick(function() { callback(null, result, ctx); });
        }
    }
};

Lifecycle.prototype._executedHandler = function(type, ctx, result, err) {
    this.trigger('execute', type, ctx, result, err);
    if (err instanceof Error) {
        this.trigger('execute:fail', type, ctx, err, result);
    } else {
        this.trigger('execute:success', type, ctx, result);
    }
};

Lifecycle.prototype.trigger = function(eventName) {
    this.emit.apply(this, arguments);
    if (_.isFunction(this.instance.emit)) {
        eventName = 'lifecycle:' + eventName;
        this.instance.emit.apply(this.instance, [eventName].concat(_.rest(arguments)));
    }
};

module.exports = Lifecycle;