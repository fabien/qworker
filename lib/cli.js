var _ = require('lodash');

module.exports = function(options) {
    options = _.extend({}, options);
    
    var cpuCount = require('os').cpus().length;
    
    var yargs = require('yargs')
        .usage('Usage: $0 --id [identifier] --mongo [url] <task ...>')
        .env('QWORKER')
        .count('verbose')
        .alias('v', 'verbose')
        .config('c', 'Configuration file')
        .alias('c', 'config')
        .describe('i', 'Worker identifier')
        .alias('i', 'id')
        .default('id', 'worker-' + process.pid)
        .describe('m', 'MongoDB connection url for mubsub')
        .alias('m', 'mongo')
        .describe('s', 'Run worker independently')
        .alias('s', 'standalone')
        .boolean('s')
        .default('standalone', false)
        .describe('p', 'Path(s) to load tasks from')
        .alias('p', 'paths')
        .describe('t', 'Tasks to load (whitespace seperated)')
        .alias('t', 'tasks');
    
    var argv = yargs.argv;
    
    if (_.has(process.env, 'NODE_APP_INSTANCE')) argv.id += '-' + process.env.NODE_APP_INSTANCE;
    
    options.verbose = _.has(options, 'verbose') ? options.verbose : argv.verbose;
    options.mongo = options.mongo || argv.mongo;
    
    options.cluster = options.cluster || Math.max(0, Math.min(argv.num || cpuCount, cpuCount));
    
    var taskIds = (argv.tasks || '').split(/\s+/);
    taskIds = _.uniq(_.compact(taskIds.concat(argv._)));
    
    if (_.isEmpty(taskIds)) {
        console.log('No tasks specified');
        process.exit(1);
    }
    
    if (_.isEmpty(options.mongo)) {
        console.log('No mongo specified');
        process.exit(1);
    }
    
    options.worker = {
        id: argv.id,
        pid: process.pid,
        paths: argv.paths,
        standalone: argv.standalone,
        taskIds: taskIds
    };
    
    return options;
};
