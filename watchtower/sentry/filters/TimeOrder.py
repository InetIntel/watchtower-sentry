"""
Filter ensures time increases monotonically.
This is useful for reordering data from sources that can provide data out of
time order.

Configuration parameters:
    interval: (number) Expected time between data points
    timeout: (number) Seconds to wait for new data to arrive before using buffer

Input:  (key, value, time)
Output:  tuples with monitonically increasing timestamps. "old" data is dropped
"""

import logging
import time
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "interval": {"type": "number"},
        "timeout": {"type": "number"},
    },
    "required": ['interval', 'timeout']
}


class TimeOrder(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("TimeOrder.__init__")
        super().__init__(config, logger, gen)
        self.interval = config['interval']
        self.timeout = config['timeout']
        self.last_key_time = {}  # last_key_time[key] = ts
        self.kv_buf = {}         # kv_buf[key][ts] = val
        self.kv_buf_timer = {}  # kv_buf_timer[key] = last_append_ts

    def _handle_kvt(self, key, val, t):
        # special case to handle first time we see a key
        if key not in self.last_key_time:
            self.last_key_time[key] = None
            self.kv_buf[key] = {}
            self.kv_buf_timer[key] = None

        # precompute some oft used values
        lkt = self.last_key_time[key]
        now = time.time()
        kbt = self.kv_buf_timer[key]
        # decide if we'll check the buffer regardless of what happens
        # i.e., because it has been a while since we last saw a value
        # for this key
        force_buffer_use = (kbt is not None) and ((kbt + self.timeout) <= now)
        # we need to check the buffer if we're going to force its use
        check_buffer = force_buffer_use

        if lkt is None or t == lkt + self.interval:
            # this is exactly the timestamp we expect to see for this key,
            # simply append it and update tracking info
            yield (key, val, t)
            self.last_key_time[key] = lkt = t
            self.kv_buf_timer[key] = now
            force_buffer_use = False
            check_buffer = True
        elif t > lkt + self.interval:
            # future data point, buffer it
            self.kv_buf[key][t] = val
        # else:
        # (self.msg_time <= lkt), too old, drop

        if check_buffer:
            # see if there are things in the buffer we can return
            buf_times = sorted(self.kv_buf[key].keys())
            for bt in buf_times:
                if force_buffer_use or bt == lkt + self.interval:
                    yield (key, self.kv_buf[key][bt], bt)
                    self.last_key_time[key] = lkt = bt
                    self.kv_buf_timer[key] = now
                    del self.kv_buf[key][bt]
                    force_buffer_use = False
                else:
                    break

    def run(self):
        logger.debug("TimeOrder.run()")
        for entry in self.gen():
            for res in self._handle_kvt(*entry):
                yield res
        # if there is anything left in the buffer, yield it now
        for key in self.kv_buf:
            for t in sorted(self.kv_buf[key]):
                yield (key, self.kv_buf[key][t], t)
