MiniQ: Lightweight Mock Batch Queue for Local Testing
=====================================================

Requirements
------------

- Python 3.7
- Pipenv

Installation
-------------

```
pipenv install -e .
```

The following commands are added to your `PATH`:
    1. miniq-server
    2. qsub
    3. qstat
    4. qdel

Start Server
--------------

First, you must start the server. 

It might be convenient to start it as a persistent, detached background process, for instance:

```
$ nohup miniq-server >& miniq.log < /dev/null &
[2] 38715

$ disown %2
```

You can set the environment variable `MINIQ_PORT` if you wish to run the server on a port other than `9876`.
