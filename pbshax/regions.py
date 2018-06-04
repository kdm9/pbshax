__all__ = ["make_regions",]

def parsefai(fai):
    with open(fai) as fh:
        for l in fh:
            cname, clen, _, _, _ = l.split()
            clen = int(clen)
            yield cname, clen


def make_regions(refpath, window=1e6, base=1):
    window = int(window)
    fai = refpath+".fai"
    windows = []
    for cname, clen in parsefai(fai):
        for start in range(0, clen, window):
            wlen = min(clen - start, window)
            windows.append("{}:{:09d}-{:09d}".format(cname, start + base, start+wlen))
    return windows

