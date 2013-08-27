#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
Task queue module for distributed async task.

+----------------+
|  Submit Task   |
+----------------+
        |
        |
       \|/
+----------------+  dispatch task   +-------------+
|                | ---------------> |             |
| Task Scheduler |                  | Task Worker |
|                | <--------------- |             |
+----------------+  task was done   +-------------+
        |
        |
       \|/
+----------------+
|    Callback    |
+----------------+

class TaskManager:
    def create_task(priority, data):
        pass

    def get_tasks():
        pass

    def get_task(task_id):
        pass

    def 

data={"the json data"}

A task statuses:

pending -> executing -> done -+-> notify
   |            |             |
   +--------<retry?> -> error +
'''

__SQL__ = '''
create table tasks (
    id varchar(50) not null,
    group_id varchar(50) not null,
    queue varchar(50) not null,
    clazz varchar(100) not null,
    name varchar(100) not null,
    timeout bigint not null,
    status varchar(50) not null,
    max_retry int not null,
    retried int not null,
    creation_time real not null,
    execution_id varchar(50) not null,
    execution_plan_time real not null,
    execution_start_time real not null,
    execution_end_time real not null,
    execution_expired_time real not null,
    version bigint not null,
    task_data text not null,
    task_result text not null,
    primary key(id),
    index(queue, execution_plan_time),
    index(queue, status)
);
'''

import os, sys, time, random, logging

from web import Dict
import db

_DEFAULT_QUEUE = 'default'

_PENDING = 'pending'
_EXECUTING = 'executing'
_ERROR = 'error'
_DONE = 'done'

def _json_dumps(obj):
    '''
    Dumps any object as json string.

    >>> class Person(object):
    ...     def __init__(self, name):
    ...         self.name = name

    >>> _json_dumps([Person('Bob'), None, [True, dict(a=1)]])
    '[{"name": "Bob"}, null, [true, {"a": 1}]]'
    '''
    def _dump_obj(obj):
        if isinstance(obj, dict):
            return obj
        d = dict()
        for k in dir(obj):
            if not k.startswith('_'):
                d[k] = getattr(obj, k)
        return d
    return json.dumps(obj, default=_dump_obj)

class TaskError(StandardError):
    pass

class ConflictError(TaskError):
    pass

def cleanup(queue=None, days=7):
    '''
    Remove tasks that already done in N days before.
    '''
    done_time = time.time() - days * 86400
    return db.update('delete from tasks where status=? and execution_end_time<?', _DONE, done_time)








def get_tasks(queue, status=None, offset=0, limit=100):
    '''
    Get tasks by queue and status.

    >>> tid1 = create_task('the_queue', 'task1', task_data='data1')
    >>> time.sleep(0.1)
    >>> tid2 = create_task('the_queue', 'task2', task_data='data2')
    >>> ts = get_tasks('the_queue')
    >>> ts[0].name
    u'task1'
    >>> ts[1].name
    u'task2'
    '''
    if offset<0:
        raise ValueError('offset must >=0')
    if limit<1 or limit>100:
        raise ValueError('limit must be 1 - 100')
    if status:
        return db.select('select * from tasks where queue=? and status=? order by execution_plan_time limit ?,?', queue, status, offset, limit)
    return db.select('select * from tasks where queue=? order by execution_plan_time limit ?,?', queue, offset, limit)

def create_task(queue, name, task_data=None, callback=None, max_retry=3, execution_plan_time=None, timeout=60):
    '''
    Create a task.

    >>> tid = create_task('sample_queue', 'sample_task_name', dict(data=1))
    >>> f = fetch_task('sample_queue')
    >>> f.id==tid
    True
    >>> f.task_data
    u'{"data": 1}'
    >>> f2 = fetch_task('sample_queue')
    >>> f2 is None
    True
    '''
    if not queue:
        queue = _DEFAULT_QUEUE
    if not name:
        name = 'unamed'
    if callback is None:
        callback = ''
    if callback and not callback.startswith('http://') and not callback.startswith('https://'):
        return dict(error='cannot_create_task', description='invalid callback')
    if max_retry < 0:
        max_retry = 0
    if timeout <= 0:
        return dict(error='cannot_create_task', description='invalid timeout')
    current = time.time()
    if execution_plan_time is None:
        execution_plan_time = current
    task = dict( \
        id=db.next_str(), \
        queue=queue, \
        name=name, \
        callback=callback, \
        timeout=timeout, \
        status=_PENDING, \
        max_retry=max_retry, \
        retried=0, \
        creation_time=current, \
        execution_id='', \
        execution_plan_time=execution_plan_time, \
        execution_start_time=0.0, \
        execution_end_time=0.0, \
        execution_expired_time=0.0, \
        task_data=task_data,
        task_result='null',
        version=0)
    db.insert('tasks', **task)
    return task['id']

def _do_fetch_task(queue, _debug=False):
    task = None
    current = time.time()
    with db.transaction():
        tasks = db.select('select * from tasks where execution_plan_time<? and queue=? and status=? order by execution_plan_time limit ?', current, queue, _PENDING, 1)
        if tasks:
            task = tasks[0]
    if not task:
        return None
    if _debug:
        time.sleep(1)
    expires = current + task.timeout
    execution_id = db.next_str()
    with db.transaction():
        if 0==db.update('update tasks set status=?, execution_id=?, execution_start_time=?, execution_expired_time=?, version=version+1 where id=? and version=?', _EXECUTING, execution_id, current, expires, task.id, task.version):
            logging.info('version conflict: expect %d.' % task.version)
            raise ConflictError()
    return Dict(id=task.id, execution_id=execution_id, queue=task.queue, name=task.name, task_data=task.task_data, version=task.version+1)

def fetch_task(queue=None, _debug=False):
    '''
    Fetch a pending task.
    '''
    if not queue:
        queue = _DEFAULT_QUEUE
    for n in range(3):
        try:
            return _do_fetch_task(queue, _debug)
        except ConflictError:
            time.sleep(random.random() / 4)
    return None

def set_task_result(task_id, execution_id, success, task_result=''):
    task = db.select_one('select id, execution_id, status, max_retry, retried from tasks where id=?', task_id)
    if task.execution_id != execution_id:
        raise TaskError('Task execution_id not match.')
    if task.status != _EXECUTING:
        raise TaskError('Task status is not executing')
    kw = dict()
    if success:
        kw['status'] = _DONE
        kw['task_result'] = task_result
    else:
        retried = task.retried + 1
        kw['retried'] = retried
        kw['status'] = _ERROR if task.retried >= task.max_retry else _PENDING
    db.update_kw('tasks', 'id=?', task_id, **kw)

def set_task_timeout(task_id):
    pass

def delete_task(task_id):
    db.update('delete from tasks where id=?', task_id)

def notify_task(task_id):
    t = db.select_one('select * from tasks where id=?', task_id)
    # post http://... content-type: application/json
    # {queue:xxx,name:xxx,result:{json}

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)
    db.init(db_type = 'mysql', \
            db_schema = 'itranswarp', \
            db_host = 'localhost', \
            db_port = 3306, \
            db_user = 'root', \
            db_password = 'passw0rd', \
            use_unicode = True, charset = 'utf8')
    print 'init mysql...'
    db.update('drop table if exists tasks')
    db.update(__SQL__)
    import doctest
    doctest.testmod()
    # multi-threading test:
    import threading
    thread_results = []
    def fetch():
        t = fetch_task('locktest', True)
        s = '%s fetched %s' % (threading.currentThread().name, t and t.name or 'None')
        thread_results.append(s)

    tid1 = create_task('locktest', 'task_1', '')
    tid2 = create_task('locktest', 'task_2', '')
    t1 = threading.Thread(target=fetch, name='Thread-001')
    t2 = threading.Thread(target=fetch, name='Thread-002')
    t3 = threading.Thread(target=fetch, name='Thread-003')
    t1.start(); t2.start(); t3.start()
    t1.join(); t2.join(); t3.join()
    print thread_results
    # testing insert 1000 tasks:
    print 'creating 1000 tasks...'
    current = time.time()
    for i in range(1000):
        create_task('multiinsert', 'task-%d' % i, 'task-data-%d' % i)
    print 'creating 1000 tasks takes %0.3f seconds.' % (time.time() - current)
    # testing fetch tasks:
    print 'fetching 1000 tasks...'
    current = time.time()
    for i in range(1000):
        t = fetch_task('multiinsert')
        if t is None:
            print 'ERROR when fetching task %d.' % i
    print 'fetching 1000 tasks takes %0.3f seconds.' % (time.time() - current)
    t = fetch_task('multiinsert')
    if t:
        print 'ERROR when fetching 1001 task.'
    #print 'cleanup...'
    #db.update('delete from tasks')
