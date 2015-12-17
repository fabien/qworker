var async = require('async');
var glob = require('glob');
var path = require('path');
var fs = require('fs-extra');
var _ = require('lodash');

exports.async = async;
exports.glob = glob;
exports.lodash = _;
exports.fs = fs;

var urlify = require('urlify').create({
    addEToUmlauts: true, spaces: '-', nonPrintable: '-', trim: true, toLower: true
});

exports.urlify = urlify;

function globAll(paths, spec, callback) {
    var results = [];
    paths = [].concat(paths || []);
    async.eachSeries(paths, function(p, next) {
        glob(path.join(p, spec), function(err, files) {
            if (err) return next(err);
            results = results.concat(files);
            next();
        });
    }, function(err) {
        callback(err, results);
    });
};

exports.globAll = globAll;

function globByPaths(paths, spec, callback) {
    var results = {};
    paths = [].concat(paths || []);
    async.eachSeries(paths, function(p, next) {
        glob(path.join(p, spec), function(err, files) {
            if (err) return next(err);
            results[p] = _.map(files, path.relative.bind(path, p));
            next();
        });
    }, function(err) {
        callback(err, results);
    });
};

exports.globByPaths = globByPaths;

function loadDefinitions(paths, callback) {
    globByPaths(paths, '**/*.json', function(err, grouped) {
        if (err) return callback(err);
        var definitions = [];
        async.each(_.keys(grouped), function(p, next) {
            async.each(grouped[p], function(filename, nextFile) {
                var meta = path.join(p, filename);
                var name = filename.replace(/\.json$/, '');
                var task = { id: name };
                task.definition = path.join(p, name) + '.js';
                fs.access(task.definition, function(err) {
                    if (err) return nextFile(err);
                    _.extend(task, require(meta));
                    task.id = urlify(task.id || task.name);
                    task.name = task.name || _.capitalize(task.id);
                    definitions.push(task);
                    nextFile();
                });
            }, next);
        }, function(err) {
            callback(err, definitions);
        });
    });
};

exports.loadDefinitions = loadDefinitions;

function extractProperties(obj) {
    var props = _.flatten(_.rest(arguments));
    var result = {};
    _.each(props, function(prop) {
        var value = _.get(obj, prop);
        if (!_.isUndefined(value)) _.set(result, prop, value);
    });
    return result;
};

exports.extractProperties = extractProperties;
