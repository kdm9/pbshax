from pbshax import parallel, make_regions
import argparse as ap
from sys import stdin, stdout, stderr, exit
from os import environ as ENV
import os


def makeregions():
    a = ap.ArgumentParser(prog="makeregions",
                          description="Print a list of regions of given size from faidx")
    a.add_argument("-s", "--size", type=int, default=1000000,
                   help="Size of each region chunk")
    a.add_argument("-b", "--base", type=int, default=1,
                   help="Coordinate system: 0 means python-style slice indexing, 1 is R-style 1-based indexing. Default 1")
    a.add_argument("-r", "--reference", type=str, required=True,
                   help="Fasta reference file (must be indexed with samtools faidx)")
    args = a.parse_args()

    regions = make_regions(args.reference, args.size, args.base)
    print(*regions, sep="\n")


def pbsparallel():
    a = ap.ArgumentParser(prog="pbsparallel",
                          description="Run commands (from stdin) in parallel (using pbsdsh).")
    a.add_argument("-p", "--procs", default=None, type=int,
                   help="Number of jobs to run in parallel (default: $PBS_NCPUS)")
    a.add_argument("-e", "--procs-per-job", default=1, type=int,
                   help="each job on input uses N processsors (on same node)")
    args = a.parse_args()

    commands = [l.strip() for l in stdin]
    try:
        parallel(commands, ncpus=args.procs, threadseach=args.procs_per_job)
    except Exception as exc:
        print(str(exc))
        exit(1)


def regionparallel():
    a = ap.ArgumentParser(prog="regionparallel",
                          description="Run a command in parallel (using pbsdsh) for each region in genome, tracking which regions have finished.")
    a.add_argument("-s", "--size", type=int, default=1000000,
                   help="Size of each region chunk")
    a.add_argument("-b", "--base", type=int, default=1,
                   help="Coordinate system: 0 means python-style slice indexing, 1 is R-style 1-based indexing. Default 1")
    a.add_argument("-p", "--procs", default=None, type=int,
                   help="Number of jobs to run in parallel (default: $PBS_NCPUS)")
    a.add_argument("-n", "--no-run", action="store_true",
                   help="Dry run: only print commands")
    a.add_argument("-r", "--reference", type=str, required=True,
                   help="Fasta reference file (must be indexed with samtools faidx)")
    a.add_argument("-f", "--regions-finished", type=str, required=False,
                   help="List of regions which have finished, which will be not be re-done.")
    a.add_argument("-x", "--regions-excluded", type=str, required=False,
                   help="List of regions which should not be run.")
    a.add_argument("-e", "--procs-per-job", default=1, type=int,
                   help="each job on input uses N processsors (on same node)")
    a.add_argument("command", type=str,
                   help="Command to run. Regions are inserted where {region} occurs (use in ouput file name).")
    args = a.parse_args()

    commands = []
    regions = make_regions(args.reference, args.size, args.base)
    for excludefile in (args.regions_finished, args.regions_excluded):
        if excludefile and os.path.exists(excludefile):
            done = set([l.strip() for l in open(excludefile)])
            regions = list(set(regions) - done)
    for region in regions:
        cmd = args.command.replace('{region}', region)
        if args.regions_finished:
            cmd += " && echo " + region + " >> " + args.regions_finished
        commands.append(cmd)
    if args.no_run:
        print(*commands, sep="\n")
    else:
        try:
            parallel(commands, ncpus=args.procs, threadseach=args.procs_per_job)
        except Exception as exc:
            print(str(exc))
            exit(1)

