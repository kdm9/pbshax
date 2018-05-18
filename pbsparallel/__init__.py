#!/usr/bin/env python3
from __future__ import print_function, division
from sys import stdin, stdout, stderr
from os import environ as ENV
import itertools as itl
import subprocess as spc
from threading import Thread
from queue import Queue


def worker(node, jobq, outq):
    while True:
        cmd = jobq.get()
        if cmd is None:
            break
        cmd = "pbsdsh -n {} -- bash -c".format(node).split() + [cmd]
        out = spc.check_output(cmd, stderr=spc.STDOUT)
        outq.put(out)
        jobq.task_done()


def outputter(outq):
    while True:
        out = outq.get()
        if out is None:
            break
        print(out)
        outq.task_done()


def parallel(commands, verbose=True, ncpu=None):
    if ncpu is None:
        ncpu = int(ENV.get('PBS_NCPUS', 1))

    jobq = Queue()
    outq = Queue()
    nodes = []
    # Add jobs
    for cmd in commands:
        jobq.put(cmd)

    # Make workers, add one poison pill per worker
    for i in range(1, ncpu+1):
        t = Thread(target=worker, args=(i, jobq, outq))
        t.start()
        nodes.append(t)
        jobq.put(None)
    outt = Thread(target=outputter, args=(outq))
    outt.start()

    # Wait for work to finish
    jobq.join()
    outq.put(None)
    outq.join()

    # join threads
    outt.join()
    for t in nodes:
        t.join()


if __name__ == "__main__":
    verbose = eval(ENV.get('PBSPAR_OUTPUT', 'True'))
    cmds = [l.strip() for l in stdin]
    parallel(cmds, verbose)
