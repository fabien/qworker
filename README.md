# Qworker

A PubSub enabled NodeJS worker for distributed task processing.

## Install

npm install -g qworker

## Usage

Qworker is a command-line tool with the following options:

```
Usage: qworker --id [identifier] --mongo [url] <task ...>

Options:
  -c, --config  Configuration file
  -i, --id      Worker identifier              [default: "worker-58987"]
  -m, --mongo   MongoDB connection url for mubsub             [required]
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

In addition to some helper methods (`log/info/warn/error/fail`) defined on
all given entities (worker, task, job), there's also the job-specific:
`job.progress` method, and the worker-related `worker.scheduleTask`, which
allows sub-tasks to be scheduled by the manager:

```
worker.scheduleTask('now', 'email', { subject: '...', to: '...' });
```

### Task metadata

- id: urlified unique identifier
- name: descriptive name
- description: task description
- jobAttributes (array): attibutes to use for notifications
- job(Start|Progress|Success|Fail)Attributes: same as above

The metadata for each task contains the absolute path to its executable code.

## Implementation

Qworker relies on [Agenda](https://github.com/rschmukler/agenda/) for handling 
tasks and their associated jobs. The MongoDB-based pubsub library
[Mubsub](https://github.com/scttnlsn/mubsub/) is used to facilitate
communication with a central management server.

The flow is as follows:

1. A worker starts and emits `worker:initialize` to the manager,
   and supplies the following info: worker id, process id and an array
   of task ids it is interested in.
2. Once a manager replies, it will validate the task ids and subsequently
   emits a reply to that specific worker (`<pid>/start`) with a configuration:
   db, collection, tasks (full metadata of requested tasks).
   If validation of any of the requested tasks fails, the worker will be 
   terminated by the manager automatically.
3. A heartbeat/ping is emitted from a worker to the manager at a set interval.
   Should the worker be online before the manager is, it will init the worker
   as in step 2 of the workflow. In other words: processes can be started and
   stopped independently. However, as long as the manager is offline, all
   workers will remain paused.
4. In case a manager is restarted, a worker will re-initialize its tasks
   and resume processing the queued jobs it is interested in.

Worker id's should obviously be unique, and this is enforced at worker startup
by communicating with the manager. There may, however, be multiple workers 
handling the same tasks, allowing a more distributed workload.

Also, because all communication is handled by a shared MongoDB instance, it is
entirely possible to distribute the tasks accross multiple servers, as long
as the executable tasks are with the (filesystem) reach of a worker.