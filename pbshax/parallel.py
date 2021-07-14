from __future__ import print_function, division
from sys import stdin, stdout, stderr
from os import environ as ENV
import itertools as itl
import subprocess as spc
from threading import Thread
from queue import Queue
from random import shuffle

PBSPARALLEL_BASECOMMAND=ENV.get("PBSPARALLEL_BASECOMMAND", "pbsdsh -n {node} -- bash -l -c")

def worker(node, jobq, outq):
    while True:
        cmd = jobq.get()
        if cmd is None:
            outq.put(None)
            break
        cmd = PBSPARALLEL_BASECOMMAND.format(node=node).split() + [cmd]
        try:
            out = spc.check_output(cmd, stderr=spc.STDOUT)
        except spc.CalledProcessError as exc:
            outq.put(exc)
            jobq.task_done()
            continue
        outq.put(out)
        jobq.task_done()


def parallel(commands, verbose=True, ncpus=None, threadseach=1):
    if ncpus is None:
        ncpus = int(ENV.get('PBS_NCPUS', 1))

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

    n_fail = 0
    exit = 0
    n_done = 0
    while True:
        if n_done == len(nodes):
            break
        out = outq.get()
        if out is None:
            n_done += 1
            outq.task_done()
            continue
        if isinstance(out, spc.CalledProcessError):
            n_fail += 1
            exit = out.returncode
            out = out.output
        if isinstance(out, bytes):
            out = out.decode("utf8")
        print(out, end="")
        outq.task_done()

    # Join threads
    for t in nodes:
        t.join()
    outq.put(None)

    if exit != 0:
        raise RuntimeError(f"{n_fail} jobs failed with exit code {exit}")
    return exit
