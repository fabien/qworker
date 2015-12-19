# Qworker

A PubSub enabled NodeJS worker for distributed task processing.

## Install

npm install -g qworker

## Usage

Qworker is a command-line tool with the following options:

```
Usage: bin/qworker --id [identifier] --mongo [url] <task ...>

Options:
  -c, --config      Configuration file
  -i, --id          Worker identifier                [default: "worker-<pid>"]
  -m, --mongo       MongoDB connection url for mubsub               [required]
  -s, --standalone  Run worker independently        [boolean] [default: false]
  -p, --paths       Path(s) to load tasks from
```

Note: you can also use environment variables with a `QWORKER_` prefix or an
optional config (json) file.

## Tasks

Tasks definitions are split in two files:

- <task-name>.json: this contains all the metadata and config settings
- <task-name>.js: this is the actual executable code for a task

A typical implementation example:

```
module.exports = function(worker, task) {
    var Proccesor = require('some/work/stuff');
    
    task.info('Defining task: %s (v.%s)', task.name, Proccesor.version);
    
    worker.task(task.id, function(job, done) {
        job.info('Running task: %s', task.name);
        var processor = new Proccesor(job);
        processor.on('progress', function(progress) {
            job.progress(progress);
        });
        processor.start(done);
    });
    
};
```

In addition to some helper methods (`log/info/warn/error`) defined on
all given entities (worker, task, job), there's also the job-specific:
`job.progress` method, and the worker-related `worker.scheduleTask`, which
allows sub-tasks to be scheduled by the manager:

```
worker.scheduleTask('now', 'email', { subject: '...', to: '...' });
```

### Task metadata

- id: urlified unique identifier (optional - defaults to urlified filename)
- name: descriptive name
- description: task description
- jobAttributes (array): attibutes to use for notifications
- job(Start|Progress|Success|Fail)Attributes: same as above
- disabled: whether to skip this task during loading (default: false)
- persistJobs: whether to keep completed (success/fail) jobs (default: false)
- maxWorkers: how many workers can subscribe to this task (default: 0, unlimited)

The metadata for each task contains the absolute path to its executable code.

By default jobAttributes is set to: 

`['data', 'lastRunAt', 'nextRunAt', 'lastFinishedAt']`

Here's an example:

```
{
  "name": "Process",
  "description": "Processes some data ...",
  "jobAttributes": ["lastFinishedAt"],
  "jobStartAttributes": ["lastFinishedAt", "data.to", "data.subject"]
}
```

## Lifecycle hooks

Several hooks are available to enable setup/teardown resources like database connections, as well as observing a task on each run. You can define multiple
hooks for each lifecycle event, as long as the names are unique for the given
instance (a single worker, per-task given multiple tasks).

When defining a hook, options can be passed like this:

```
initalizer('foo', { foo: 'bar' }, callback);
```

A hook's callback function can be either synchronous (`ctx` arg), or
asynchronous (`ctx` and `callback` arguments).

The callback's `ctx` argument contains the following metadata:

- instance: a reference to the current instance (worker or task)
- type: the current hook type (initalizer, finalizer, observer)
- name: the given name of the hook
- options: options passed during the hook definition call, if any
- job: a Job instance, in case of a Task observer hook

The following hooks are triggered when the worker starts:

```
worker.initializer('db-connect', function(ctx, next) {
    ctx.instance.log('Initializing: %s', ctx.name);
    setTimeout(next, 100);
});

task.initializer('setup', function(ctx) {
    ctx.instance.log('Initializing: %s - %s', ctx.id, ctx.name);
});

```

These hooks are triggered when the worker stops/terminates:

```
worker.finalizer('db-disconnect', function(ctx, next) {
    ctx.instance.log('Finalizing: %s', ctx.name);
    setTimeout(next, 100);
});

task.finalizer('cleanup', function(ctx) {
    ctx.instance.log('Finalizing: %s - %s', task.id, ctx.name);
});
```

And finally the Task-specific `observer` hook:

```
task.observer('execute', function(ctx, next) {
    ctx.instance.log('Observing: %s - %s (%s)', ctx.instance.id, ctx.name, ctx.job.attrs._id);
    setTimeout(next, 100);
});
```

## Pubsub Events

- `worker:initialize`
- `worker:start`
- `worker:stop`
- `worker:terminate`
- `worker:ping`
- `worker:notification`

## Implementation

Qworker relies on [Agenda](https://github.com/rschmukler/agenda/) for handling 
tasks and their associated jobs. The MongoDB-based pubsub library
[Mubsub](https://github.com/scttnlsn/mubsub/) is used to facilitate
communication with a central management server.

The flow is as follows:

1. A worker starts and emits `worker:initialize` to the manager,
   and supplies the following info: worker id, process id and an array
   of task ids it is interested in.
2. The manager will validate the task ids and subsequently emits a reply to
   that specific worker (`<pid>/start`) with a configuration:
   db, collection, tasks (full metadata of requested tasks).
   If validation of any of the requested tasks fails, the worker will be 
   terminated by the manager automatically.
3. A heartbeat/ping is emitted from a worker to the manager at a set interval.
   Should the worker be online before the manager is, it will init the worker
   as in step 2 of the workflow. In other words: processes can be started and
   stopped independently. However, as long as the manager is offline, all
   workers will remain paused. The ping is checked at `interval * 2` to
   confirm that the active workers are still available.
4. In case a manager is restarted, a worker will re-initialize its tasks
   and resume processing the queued jobs it registered.

Worker id's should obviously be unique, and this is enforced at worker startup
by communicating with the manager. There may, however, be multiple workers 
handling the same tasks, allowing a more distributed workload.

Also, because all communication is handled by a shared MongoDB instance, it is
entirely possible to distribute the tasks accross multiple servers, as long
as the executable tasks are within the (filesystem) reach of a worker.

## Good to know

- Tasks should generally only be scheduled through worker.scheduleTask,
  unless the local agenda instance happens to know about this task.
- Since you have access to both Agenda (`worker.agenda`) and Mubsub
  (`worker.mubsub`) it's possible to communicate between workers, or
  to trigger different pubsub events for other services.
- Any service subscribing to the mubsub collection will be notified.  