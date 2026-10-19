import sys
import json
import logging
import math
import random

loghandler = logging.StreamHandler()
loghandler.setFormatter(logging.Formatter(
    '%(asctime)s.%(msecs)03d '
    '%(name)-20s '
    '%(threadName)-10s '
    '%(levelname)-8s: %(message)s',
    '%H:%M:%S'))
rootlogger = logging.getLogger()
rootlogger.addHandler(loghandler)
rootlogger.setLevel(logging.DEBUG)

sys.path.append("watchtower/sentry")
sys.path.append(".")
from watchtower.sentry.sentry import Sentry

def interleave(lists):
    result = []
    n = max(map(len, lists))
    for i in range(0, n):
        for lst in lists:
            try:
                result.append(lst[i])
            except IndexError:
                pass
    return result

timebase = 1000000000
timestep = 10
steps = 200
warmup_steps = 5
history_steps = 60
inpaint_steps = 40
shift_step = 70
median_normalize = False

indata = []
outdata = []

params = {
    #'group': [[baseline, period, amplitude], ...],
    'outage': [[14000, 15, 30], [12000, 10, 10]],
    'hole':   [[23000, 20, 40], [23000, 12, 10]],
    'shift':  [[35000, 30, 50], [31000, 15, 10]],
    'order':  [[43000, 40, 60], [39000, 17, 10]],
}

history = dict()
history_start = dict()
exp_aggsum = dict() # expected aggsum
exp_median = dict() # expected bounds of median
for group in params:
    history[group] = list()
    history_start[group] = 0
    exp_aggsum[group] = list()
    exp_median[group] = list()

# compute the times
times = [timebase + i * timestep for i in range(0, steps)]

# make up some data
for group in params:
    if group == "order":
        random.seed(1)
        random.shuffle(times)

    for i in range(0, steps):
        aggsum = 0
        inpaint = False

        for prober, (baseline, period, amplitude) in \
                enumerate(params[group], start=1):
            # baseline = amplitude # debug

            # generate "normal" data
            value = baseline + (amplitude * math.sin(i*2*math.pi/period))
            key = 'aaa.%s.prober-%d.zzz' % (group, prober)

            # throw in some outliers
            if group == "outage" and prober == 1 and i in range(50,60):
                # outage
                value *= 0.3
                inpaint = True
            if group == "shift" and prober == 2 and i >= shift_step:
                # level shift
                value += 3 * baseline
                if i < shift_step + inpaint_steps:
                    inpaint = True
                elif i == shift_step + inpaint_steps:
                    # rewrite history to undo inpainting
                    for j in range(shift_step, len(history[group])):
                        history[group][j] = exp_aggsum[group][j]
                    history_start[group] = shift_step
            if group == "hole" and prober == 2 and i in range(73,76):
                # missing data
                continue

            value = int(value)
            aggsum += value
            indata.append((key, value, times[i]))

            # print("%3d %s %s" % (i, key, value*"#")) # debug

        # calculate expected median
        if i < warmup_steps:
            median = None
        else:
            start = history_start[group]
            if i - history_steps > start:
                start = i - history_steps
            rank = math.ceil((i - start)/2) - 1
            mhist = sorted(history[group][start:])
            median = mhist[rank]
            #print("mhist[%d] rank=%d median=%d: %r" % (i, rank, median, mhist))

        # store expected aggsum and median
        exp_aggsum[group].append(aggsum)
        exp_median[group].append(median)
        # store history needed for median
        history[group].append(aggsum if not inpaint else median)


####################################################################
# Test 1: basic functionality: simple config, module chaining, etc.

cfg = {
    "loglevel": "INFO",
    "pipeline": [{
        "module": "sources.DataIn",
        "input": indata,
    }, {
        "module": "sinks.DataOut",
        "output": outdata,
    }]
}

outdata.clear()
s = Sentry(None, cfg)
s.run()

assert outdata == indata

####################################################################
# Test strict time ordering
cfg['pipeline'].insert(-1, {
        "module": "filters.TimeOrder",
        "interval": timestep,
        "timeout": timestep * 2,
    })
outdata.clear()
s = Sentry(None, cfg)
s.run()

# check output timestamps are monotonically increasing by key
lt = {}
for (k, v, t) in outdata:
    if k not in lt:
        lt[k] = t
        continue
    assert t > lt[k]
    lt[k] = t

# ensure input and output are equal after sorting
# this is tricky since data that is older than the first timestamp is dropped
filtered_in = []
ft = {}
for (k, v, t) in indata:
    if k not in ft:
        ft[k] = t
    if t >= ft[k]:
        filtered_in.append((k, v, t))

assert len(filtered_in) == len(outdata)
assert sorted(filtered_in) == sorted(outdata)

print("TimeOrder test passed")


####################################################################
# Test 2: aggregating by group

cfg['pipeline'].insert(-1, {
        "module": "filters.AggSum",
        "expression": "aaa.(*).*.zzz",
        "groupsize": 2,
        "timeout": 1,
        "droppartial": False,
    })

outdata.clear()
s = Sentry(None, cfg)
s.run()

results = dict()
prev_time = dict()
for i, entry in enumerate(outdata):
    key, value, t = entry
    group = key.split(sep='.')[1]
    if group not in results:
        results[group] = dict()
    results[group][t] = value
    # make sure output for key is ordered by time
    if key in prev_time:
        assert t == prev_time[key] + timestep, \
            "result #%d, key %r: time %r should be == %d" % \
            (i, key, t, prev_time[key] + timestep)
    prev_time[key] = t

for group in params:
    if group == "order":
        # TODO: figure out how to test this
        continue
    assert len(results[group]) == steps, \
        "group %r: result count %d != expected %d" % \
        (group, len(results[group]), steps)

for entry in outdata:
    key, value, t = entry
    i = (t - timebase) // timestep
    group = key.split(sep='.')[1]
    if group == "order":
        # TODO: figure out how to test this
        continue
    assert exp_aggsum[group][i] == value, \
        "key %r, time %r: value %d != expected %r" % \
        (key, t, value, exp_aggsum[group][i])

print("AggSum test passed")


####################################################################
# Test 3: moving median
#

cfg['pipeline'].insert(-1, {
        "module": "filters.MovingStat",
        "type": ['median'],
        "warmup": warmup_steps * timestep,
        "history": history_steps * timestep,
        "inpainting": {
            "min": 0.8,
            "max": 2,
            "maxduration": inpaint_steps * timestep,
        },
        "normalize": median_normalize,
    })
#cfg['loglevel'] = 'DEBUG'

outdata.clear()
s = Sentry(None, cfg)
s.run()

results = dict()
prev_time = dict()
for i, entry in enumerate(outdata):
    key, value, t = entry
    group = key.split(sep='.')[1]
    if group not in results:
        results[group] = dict()
    results[group][t] = value
    # make sure output for key is ordered by time
    if key in prev_time:
        assert t > prev_time[key], \
            "result #%d, key %r: time %r should be > %d" % \
            (i, key, t, prev_time[key])
    prev_time[key] = t

for group in params:
    if group == "order":
        # TODO: figure out how to test this
        continue
    assert len(results[group]) == steps - warmup_steps, \
        "group %r: result count %d != expected %d" % \
        (group, len(results[group]), steps - warmup_steps)

for entry in outdata:
    key, value, t = entry
    i = (t - timebase) // timestep
    group = key.split(sep='.')[1]
    if group == "order":
        # TODO: figure out how to test this
        continue
    if median_normalize:
        exp = exp_aggsum[group][i] / exp_median[group][i]
    else:
        exp = exp_median[group][i]

    assert value == exp, \
        "key %r, time %r: value %r != expected %r" % \
        (key, t, value, exp)

#for entry in outdata:
#    json.dump(entry, sys.stdout)
#    print()

print("MovingStat test passed")


####################################################################
# Test 4: AggSum fleet-level prober activity tracking
#
# Fleet has 3 probers (prober-1, prober-2, prober-3).
# Series 'full' is probed by all 3 probers (expected sum = 10+20+30 = 60).
# Series 'sparse' is probed only by prober-1 and prober-2 (expected sum = 10+20 = 30).
# prober-3 never probes 'sparse' (null result).

test4_indata = []
t0 = 2000000000
# t0: Baseline healthy round (dropped by dropfirst)
test4_indata.append(('probe.full.prober-1.cnt', 10, t0))
test4_indata.append(('probe.full.prober-2.cnt', 20, t0))
test4_indata.append(('probe.full.prober-3.cnt', 30, t0))
test4_indata.append(('probe.sparse.prober-1.cnt', 10, t0))
test4_indata.append(('probe.sparse.prober-2.cnt', 20, t0))

# t1: All 3 probers report globally.
# 'full' receives 3/3 -> emitted (60).
# 'sparse' receives 2/3, but prober-3 is null -> emitted (30).
t1 = t0 + 10
test4_indata.append(('probe.full.prober-1.cnt', 10, t1))
test4_indata.append(('probe.full.prober-2.cnt', 20, t1))
test4_indata.append(('probe.full.prober-3.cnt', 30, t1))
test4_indata.append(('probe.sparse.prober-1.cnt', 10, t1))
test4_indata.append(('probe.sparse.prober-2.cnt', 20, t1))

# t2: prober-3 suffers network failure (does not report anywhere in fleet).
# 'full' receives 2/3 (missing prober-3 which failed globally) -> dropped!
# 'sparse' receives 2/2 (prober-3 is not in sparse) -> emitted (30).
t2 = t0 + 20
test4_indata.append(('probe.full.prober-1.cnt', 10, t2))
test4_indata.append(('probe.full.prober-2.cnt', 20, t2))
test4_indata.append(('probe.sparse.prober-1.cnt', 10, t2))
test4_indata.append(('probe.sparse.prober-2.cnt', 20, t2))

# t3: prober-2 suffers network failure. Only prober-1 and prober-3 report.
# 'full' is missing prober-2 -> dropped!
# 'sparse' is missing prober-2 -> dropped!
t3 = t0 + 30
test4_indata.append(('probe.full.prober-1.cnt', 10, t3))
test4_indata.append(('probe.full.prober-3.cnt', 30, t3))
test4_indata.append(('probe.sparse.prober-1.cnt', 10, t3))

# t4: All 3 probers report. 'full' has a real target outage (0, 0, 0).
# Both 'full' (0) and 'sparse' (30) -> emitted.
t4 = t0 + 40
test4_indata.append(('probe.full.prober-1.cnt', 0, t4))
test4_indata.append(('probe.full.prober-2.cnt', 0, t4))
test4_indata.append(('probe.full.prober-3.cnt', 0, t4))
test4_indata.append(('probe.sparse.prober-1.cnt', 10, t4))
test4_indata.append(('probe.sparse.prober-2.cnt', 20, t4))

# Late straggler arrives for t1 after t1 was already processed/emitted
test4_indata.append(('probe.full.prober-1.cnt', 999, t1))

test4_outdata = []
cfg4 = {
    "loglevel": "INFO",
    "pipeline": [{
        "module": "sources.DataIn",
        "input": test4_indata,
    }, {
        "module": "filters.AggSum",
        "expression": "probe.(*).*.cnt",
        "prober_regex": r"\.(prober-\d+)\.",
        "groupsize": 3,
        "timeout": 1,
        "droppartial": True,
        "dropfirst": True,
    }, {
        "module": "sinks.DataOut",
        "output": test4_outdata,
    }]
}

s4 = Sentry(None, cfg4)
s4.run()

expected_out = {
    ("probe.full.*.cnt", t0): 60,
    ("probe.full.*.cnt", t1): 60,
    ("probe.sparse.*.cnt", t1): 30,
    ("probe.sparse.*.cnt", t2): 30,
    ("probe.full.*.cnt", t4): 0,
    ("probe.sparse.*.cnt", t4): 30,
}

actual_out = {(k, t): v for (k, v, t) in test4_outdata}

assert actual_out == expected_out, \
    "Test 4 failed!\nExpected: %r\nActual:   %r" % (expected_out, actual_out)

print("AggSum fleet tracking test passed")

####################################################################
print("All tests passed.")
