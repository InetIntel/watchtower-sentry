"""Sink that writes (k,v,t) tuples to an in-memory array.

Intended for testing.

Configuration parameters ('*' indicates required parameter):
    output*: (list) where output will be written

Input:  (key, value, time)

Sink result:  tuples written to specified list in memory.
"""

import logging
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "output": {"type": "array"}
    },
}

class DataOut(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("DataOut.__init__")
        super().__init__(config, logger, gen)
        self.output = config['output']

    def run(self):
        logger.debug("DataOut.run()")
        for entry in self.gen():
            key, value, t = entry
            key = str(key, 'ascii')
            self.output.append((key, value, t))
        logger.debug("DataOut.run() done")
