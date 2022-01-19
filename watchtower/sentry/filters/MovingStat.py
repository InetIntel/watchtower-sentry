"""Filter that calculates relative distance from a moving statistic

Configuration parameters ('*' indicates required parameter):
    type*: array of a statistic type name and optional integer parameters
        ['mean']             mean of values
        ['quantile', k, q]   k'th q-quantile of values
        ['median']           middle value; equivalent to ['quantile', 1, 2]
        ['min']              minimum value; equivalent to ['quantile', 0, 1]
        ['max']              maximum value; equivalent to ['quantile', 1, 1]
    history*: (integer) Number of seconds of data over which to calculate.
    warmup*: (integer) Minimum number of seconds of data to collect before
        generating output.
    includeabsolute: (boolean) Emit absolute values alongside relative
    minprediction: (number) Minimum prediction value before output is generated
    inpainting:
        min: (number <1.0) inpaint if (value/stat) falls below this value
        max: (number >1.0) inpaint if (value/stat) rises above this value
        maxduration*: (integer) maximum time (in seconds) to inpaint.  If this
            time is exceeded, the previously inpainted values are replaced
            with their original values for purposes of calculating the stat.
            I.e., the values previously considered extreme will now be
            considered the new normal.

Output context variables: method

Input:  (key, value, time)

Output:  (key, value*, time)
    key is the same as input key.
    value is the ratio of the input value to the statistic for all values for
        the same key where (old.time > new.time - history). If includeabsolute
        is set, then value is a triple of (ratio, actual, predicted).
    time is the same as input time.
"""

import logging
import bisect
import time
from collections import deque
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "type": {
            "type": "array",
            # first item is stattype name, other are parameters
            "items": [{"type": "string"}],
            "additionalItems": {"type": "integer"},
            "minItems": 1
        },
        "history":       {"type": "integer", "exclusiveMinimum": 0},
        "warmup":        {"type": "integer", "exclusiveMinimum": 0},
        "normalize": {"type": "boolean"},  # for testing/debugging
        "includeabsolute": {"type": "boolean"},
        "minprediction": {"type": "number"},
        "inpainting":    {
            "type": "object",
            "properties": {
                "min":         {"type": "number", "exclusiveMaximum": 1},
                "max":         {"type": "number", "exclusiveMinimum": 1},
                "maxduration": {"type": "integer", "exclusiveMinimum": 0},
            },
            "additionalProperties": False,
            "required": ["maxduration"],
        },
    },
    "required": ["type", "history", "warmup"],
}


def _sortedlist_add_remove(slist, additem, rmitem):
    """ Remove one item from a sorted list and add another item. """

    # Find the set of values between the value being removed and the point
    # where the new value will be inserted, and shift them towards the
    # removed value, thus overwriting the removed value and making a hole
    # to insert the new value.  On average, with a list of length N, this
    # algorithm will need to shift ~N/3 items (compared to N for the most
    # naive remove-then-insert algorithm).  (Some kind of tree would
    # probably be more efficient, though maybe not by as much as one might
    # expect, due to greater overhead.  And there are no trees in the
    # python standard library.  Consider
    # http://www.grantjenks.com/docs/sortedcontainers/.)
    ## logger.debug("slist:  %r", slist)
    if rmitem < additem:
        left = bisect.bisect_right(slist, rmitem)
        right = bisect.bisect_left(slist, additem, lo=left)
        logger.debug("rm=%d,add=%d: left=%d, right=%d",
            rmitem, additem, left, right)
        slist[left-1:right-1] = slist[left:right]
        slist[right-1] = additem
    elif additem < rmitem:
        left = bisect.bisect_right(slist, additem)
        right = bisect.bisect_left(slist, rmitem, lo=left)
        logger.debug("add=%d,rm=%d: left=%d, right=%d",
            additem, rmitem, left, right)
        slist[left+1:right+1] = slist[left:right]
        slist[left] = additem
    #else: # removing and inserting the same value is a no-op
        #logger.debug("add=%d,rm=%d: no-op", additem, rmitem)


class MovingStat(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("MovingStatistic.__init__")
        super().__init__(config, logger, gen)
        self.config = config
        self.warmup = config['warmup']

        self.normalize = config.get('normalize', True)
        self.include_absolute = config.get('includeabsolute', False)
        if not self.normalize and self.include_absolute:
            raise SentryModule.UserError("module %s: normalize must be set if "
                                         "includeabsolute is set" % self.modname)

        self.history_duration = config['history']
        if self.history_duration <= self.warmup:
            raise SentryModule.UserError('module %s: history (%d) must be '
                'greater than warmup (%d)' %
                (self.modname, self.history_duration, self.warmup))

        self.min_prediction = config.get('minprediction', None)

        if 'inpainting' in config:
            inp = config['inpainting']
            self.inpaint_maxduration = inp.get('maxduration', None)
            self.inpaint_min = inp.get('min', None)
            self.inpaint_max = inp.get('max', None)
            self.should_inpaint = self.ratio_is_extreme
            if self.inpaint_maxduration < self.warmup:
                logger.warning("module %s: inpainting.maxduration (%d) < "
                               "warmup (%d), which may lead to gaps in output"
                               % (self.modname, self.inpaint_maxduration, self.warmup))
        else:
            self.inpaint_maxduration = None
            self.inpaint_min = None
            self.inpaint_max = None
            self.should_inpaint = lambda ratio: False

        stattype = config['type'][0]
        n_params = 2 if stattype == "quantile" else 0
        if len(config['type']) - 1 != n_params:
            raise SentryModule.UserError("module %s: type %s expects %d "
                "parameters (found %d)"
                % (self.modname, stattype, n_params, len(config['type']) - 1))

        stattype_params = {
            "mean":     [MovingStat.Mean, None, None],
            "min":      [MovingStat.Quantile, 0, 1],
            "max":      [MovingStat.Quantile, 1, 1],
            "median":   [MovingStat.Quantile, 1, 2],
            "quantile": [MovingStat.Quantile, *config['type'][1:]],
        }
        self.statclass, self.k, self.q = stattype_params[stattype]
        if self.q and self.k > self.q:
            raise SentryModule.UserError("module %s: %s: first "
                "number (%d) must be <= second (%d)"
                % (self.modname, stattype, self.k, self.q))

        ctx['method'] = ', '.join(config['type']) # for AlertKafka

        self.data = dict()
        self.last_key_time = dict()

    class StatBase:
        def __init__(self, ms_ctx):
            self.ms_ctx = ms_ctx
            self.vtq = deque()  # list of (v,t) ordered by t (maybe inpainted)
            self.raw_vtq = None # list of raw (v,t) collected while inpainting

    class Quantile(StatBase):
        def __init__(self, ms_ctx):
            super().__init__(ms_ctx)
            logger.debug("init quantile: %d/%d", self.ms_ctx.k, self.ms_ctx.q)
            self.values = None # sorted list of values

        def is_initialized(self):
            return self.values is not None

        def reset(self):
            self.values = None
            logger.debug("reset")

        def initialize(self):
            self.values = sorted([v for v, t in self.vtq])
            logger.debug("sorted: %r", self.values)

        def insert_remove(self, ins_val, rm_val):
            _sortedlist_add_remove(self.values, ins_val, rm_val)
            logger.debug("values: %r", self.values)

        def remove(self, val):
            self.values.remove(val)

        def insert(self, val):
            bisect.insort(self.values, val)
            logger.debug("values: %r", self.values)

        def prediction(self):
            # Nearest rank method: smallest value such that no more than k/q
            # of the data is < value and at least k/q of the data is <= value
            if self.ms_ctx.k == 0:
                rank = 0
            else:
                N = len(self.values)
                if N == 0:
                    return None
                # -(-N*k//q) is equivalent to ceil(N*k/q), but faster
                rank = -(-N * self.ms_ctx.k // self.ms_ctx.q) - 1
            return self.values[rank]

    class Mean(StatBase):
        def __init__(self, ms_ctx):
            super().__init__(ms_ctx)
            self.sum = None # sum of values

        def is_initialized(self):
            return self.sum is not None

        def reset(self):
            self.sum = None
            logger.debug("reset")

        def initialize(self):
            self.sum = sum([v for v, t in self.vtq])
            logger.debug("init: %r / %r", self.sum, len(self.vtq))

        def insert_remove(self, ins_val, rm_val):
            self.sum -= rm_val
            self.sum += ins_val
            logger.debug("mean: %r / %r", self.sum, len(self.vtq))

        def remove(self, val):
            self.sum -= val
            logger.debug("mean: %r / %r", self.sum, len(self.vtq))

        def insert(self, val):
            self.sum += val
            logger.debug("mean: %r / %r", self.sum, len(self.vtq))

        def prediction(self):
            return self.sum / len(self.vtq)


    def ratio_is_extreme(self, ratio):
        if ratio is None:
            return False
        if (self.inpaint_min and ratio < self.inpaint_min):
            logger.debug("ratio %f < min %f", ratio, self.inpaint_min)
            return True
        if (self.inpaint_max and ratio > self.inpaint_max):
            logger.debug("ratio %f > max %f", ratio, self.inpaint_max)
            return True
        return False

    def run(self):
        logger.debug("MovingStatistic.run()")
        last_size_log = None
        for entry in self.gen():
            logger.debug("MD: %s", str(entry))
            key, value, t = entry

            if value is None:
                continue

            if key not in self.data:
                data = self.statclass(self)
                self.data[key] = data
                self.last_key_time[key] = None
            else:
                data = self.data[key]

            # Ensure timestamps for a key increase monotonically
            lkt = self.last_key_time[key]
            if lkt is not None and t <= self.last_key_time[key]:
                logging.warning("MovingStat: out-of-order: (%s, %s, %s) last_time: %s" %
                                (key, value, t, self.last_key_time[key]))
            self.last_key_time[key] = t

            # log the number of series we're tracking every 60s
            # TODO: consider making this configurable
            now = time.time()
            if last_size_log is None or (last_size_log + 60) <= now:
                logging.info("MovingStat: tracking %d keys" % len(self.data))
                last_size_log = now

            if not data.vtq or data.vtq[0][1] > t - self.warmup:
                # not enough points yet.  Just store the new value.
                data.vtq.append((value, t))
                continue

            window_start = t - self.history_duration

            if not data.is_initialized():
                # Warmup is done; initialize data (not including the new value)
                data.initialize()

            # If window is overfull, remove old items.  This can happen when
            # there's a time gap in new arrivals.
            while data.vtq and data.vtq[0][1] < window_start:
                oldest = data.vtq.popleft()
                logger.warning("removing extra old item (%s, %d, %d)",
                    key, oldest[0], oldest[1])
                data.remove(oldest[0])

            # Calculate predicted value based on data in the window (not
            # including the new value)
            predicted = data.prediction()
            if self.min_prediction is not None and \
                    predicted is not None and \
                    predicted < self.min_prediction:
                # predicted value is too low
                continue
            ratio = value/predicted if predicted else None
            logger.debug("predicted=%r, value=%r, ratio=%r",
                predicted, value, ratio)

            newval = value

            inpaint_started = data.raw_vtq[0][1] if data.raw_vtq else None
            if self.should_inpaint(ratio):
                if not inpaint_started:
                    # Start inpainting
                    logger.debug("### extreme value: start inpainting")
                    data.raw_vtq = deque()
                    data.raw_vtq.append((value, t))
                    newval = predicted
                elif inpaint_started > t - self.inpaint_maxduration:
                    # Continue inpainting
                    logger.debug("### extreme value: continue inpainting")
                    data.raw_vtq.append((value, t))
                    newval = predicted
                else:
                    # Extreme is the new normal.  Discard old normal and
                    # inpainted values, and rebuild history using raw values
                    # that had previously been considered extreme.
                    logger.debug("### extreme value: new normal")
                    data.vtq = data.raw_vtq
                    data.raw_vtq = None
                    if data.vtq[0][1] > t - self.warmup:
                        # Not enough data
                        data.reset()
                        data.vtq.append((value, t))
                        continue
                    data.initialize() # not including the new value
                    # Recalculate prediction using restored raw data
                    predicted = data.prediction()
                    ratio = newval/predicted if predicted else None
                    logger.debug("new predicted=%r, value=%r, ratio=%r",
                        predicted, value, ratio)
            elif inpaint_started:
                # We were inpainting, but new value is not extreme.
                # Leave old inpainted values in history and forget buffered
                # raw values.
                logger.debug("### return to normal: cancel inpainting")
                data.raw_vtq = None

            data.vtq.append((newval, t))

            if data.vtq[0][1] > window_start:
                # Window is not full.  Insert newval into the sorted list.
                logger.debug("insert %d", newval)
                data.insert(newval)
            else:
                # Window is full.  Remove the oldest value and insert the new
                # value (which may be raw or inpainted).
                oldest = data.vtq.popleft()
                data.insert_remove(newval, oldest[0])

            # if include_absolute is True, then normalize is also True
            if not self.normalize:
                yield (key, predicted, t)
            else:
                yield (key, ratio if not self.include_absolute else (ratio, value, predicted), t)
