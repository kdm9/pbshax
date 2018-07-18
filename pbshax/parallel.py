from __future__ import print_function, division
from sys import stdin, stdout, stderr
from os import environ as ENV
import itertools as itl
import subprocess as spc
from threading import Thread
from queue import Queue
from random import shuffle


def worker(node, jobq, outq):
    while True:
        cmd = jobq.get()
        if cmd is None:
            break
        cmd = "pbsdsh -n {} -- bash -l -c".format(node).split() + [cmd]
        try:
            out = spc.check_output(cmd, stderr=spc.STDOUT)
        except spc.CalledProcessError as exc:
            outq.put(exc.output)
            jobq.task_done()
            break
        outq.put(out)
        jobq.task_done()


def outputter(outq):
    while True:
        out = outq.get()
        if out is None:
            break
        if isinstance(out, bytes):
            out = out.decode("utf8")
        print(out, end="")
        outq.task_done()


def parallel(commands, verbose=True, ncpus=None, threadseach=1):
    if ncpus is None:
        ncpus = int(ENV.get('PBS_NCPUS', 1))

    list(commands)
    jobq = Queue()
    outq = Queue()
    nodes = []
    # Add jobs
    for cmd in commands:
        jobq.put(cmd)

    # Make workers, add one poison pill per worker
    for i in range(1, ncpus+1, threadseach):
        t = Thread(target=worker, args=(i, jobq, outq))
        t.start()
        nodes.append(t)
        jobq.put(None)
    outt = Thread(target=outputter, args=(outq,))
    outt.start()

    # Join threads
    for t in nodes:
        t.join()
    outq.put(None)
    outt.join()
