import psutil

pids = psutil.pids()
for p in pids:
    print psutil.Process(p).cmdline()