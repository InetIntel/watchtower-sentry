"""Filter that aggregates values into time "bin" and reports the median
observed value -- i.e. if a series produces values once per minute, but we
want to perform event detection on the median of each 5 minute bin then we
would use this filter.

The median for each observed key is tracked separately.

Configuration parameters:
    timebin: (integer) The size of the aggregate time bin in seconds. Default
             is 300 (i.e. 5 minutes)
    dropfirst: (boolean) If set to true, ignore the first time bin as it
               may be incomplete. Default is True.

Input: (key, value, time)

Output: (key, median, time bin)

"""
import logging
import time
import statistics
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "timebin": { "type": "integer", "exclusiveMinimum": 60 },
        "dropfirst": { "type": "boolean" }
    },
    "required": []
}

class AggTimeMedian(SentryModule.SentryModule):

    def __init__(self, config, gen, ctx):
        logger.debug("AggTimeMedian.__init__")
        super().__init__(config, logger, gen)
        self.timebin = config.get('timebin', 300)
        self.dropfirst = config.get('dropfirst', True)
        self.firstbin = None
        self.active_keys = {}
        self.currentbin = None

    def _get_bin_from_timestamp(self, t):
        return int(int(t) / int(self.timebin)) * self.timebin

    def run(self):
        logger.debug("AggTimeMedian.run()")
        for entry in self.gen():
            logger.debug("ATM: %s", entry)
            key, value, t = entry

            tbin = self._get_bin_from_timestamp(t)
            if self.firstbin is None:
                self.firstbin = tbin
                self.currentbin = tbin

            if key not in self.active_keys:
                self.active_keys[key] = {
                        "bin": tbin,
                        "values": []
                }

            if tbin < self.currentbin:
                logger.error("Seen old data for key %s -- timestamp was %d, but working on bin %d", key, t, currentbin)
                continue

            if tbin == self.currentbin:
                self.active_keys[key]['values'].append(value)
                continue

            # assuming our data all arrives in time order, we can then
            # treat the first datapoint with a timestamp after our current
            # bin as an indication that ALL of our keys have seen their last
            # datapoint for the current bin and can therefore yield their
            # median values back into the pipeline
            for k, v in self.active_keys.items():
                if not self.dropfirst or self.currentbin != self.firstbin:
                    if len(v['values']) != 0 and v['bin'] == self.currentbin:
                        median = statistics.median(sorted(v['values']))
                        yield (k, median, self.currentbin)
                v['values'] = []
                v['bin'] = tbin
            self.active_keys[key]['values'].append(value)
            self.currentbin = tbin

        logger.debug("AggTimeMedian.run() done")
