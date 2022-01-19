"""Sink that writes (k,v,t) tuples to a JSON file.

Configuration parameters ('*' indicates required parameter):
    file: (string) Name of output file.  If "-" or omitted, write to stdout.
    compact: (boolean, default true)

Input:  (key, value, time)

Sink result:  tuples written to specified file.
"""

import sys
import logging
import json
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"},
        "compact": {"type": "boolean"}
    },
}

class JsonOut(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("JsonOut.__init__")
        super().__init__(config, logger, gen)
        self.filename = config.get('file', '-')
        self.separators = (',', ':') if config.get('compact', True) \
            else (', ', ': ')

    def run(self):
        logger.debug("JsonOut.run()")
        f = sys.stdout
        try:
            if self.filename != '-':
                f = open(self.filename, 'w')
            for entry in self.gen():
                key, value, t = entry
                key = str(key, 'ascii')
                json.dump((key, value, t), f, separators=self.separators)
                f.write('\n')
        finally:
            if f is not sys.stdout:
                f.close()
        logger.debug("JsonOut.run() done")
