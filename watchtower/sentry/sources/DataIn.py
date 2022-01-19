"""Source that reads (k,v,t) tuples from an in-memory object.

Intended for testing.

Configuration parameters ('*' indicates required parameter):
    input: (array) Array of k,v,t tuples.

Output context variables: expression

Output:  (key, value, time)
"""

import logging
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "input": {"type": "array"},
    }
}

class DataIn(SentryModule.Source):

    def __init__(self, config, gen, ctx):
        logger.debug("DataIn.__init__")
        super().__init__(config, logger, gen)
        self.data = config['input']
        ctx['expression'] = '<data>' # for AlertKafka

    def run(self):
        logger.debug("DataIn.run()")
        for entry in self.data:
            key, value, t = entry
            key = bytes(key, 'ascii')
            yield (key, value, t)

        logger.debug("DataIn.run() done")
