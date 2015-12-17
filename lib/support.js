var async = require('async');
var glob = require('glob');
var path = require('path');
var _ = require('lodash');

exports.async = async;
exports.glob = glob;
exports.lodash = _;

exports.urlify = require('urlify').create({
    addEToUmlauts: true, spaces: '-', nonPrintable: '-', trim: true, toLower: true
});

exports.globAll = function globAll(paths, spec, callback) {
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

exports.extractProperties = function extractProperties(obj) {
    var props = _.flatten(_.rest(arguments));
    var result = {};
    _.each(props, function(prop) {
        var value = _.get(obj, prop);
        if (!_.isUndefined(value)) _.set(result, prop, value);
    });
    return result;
};
