"""Source that reads (k,v,t) tuples from a JSON file.

Configuration parameters ('*' indicates required parameter):
    file: (string) Name of input file.  If "-" or omitted, read from stdin.

Output context variables: expression

Output:  (key, value, time)
"""

import logging
import json
import fileinput
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "file": {"type": "string"},  # omitted or "-" means stdin
    }
}

class JsonIn(SentryModule.Source):

    def __init__(self, config, gen, ctx):
        logger.debug("JsonIn.__init__")
        super().__init__(config, logger, gen)
        self.filenames = [config['file']] if 'file' in config else []
        ctx['expression'] = config.get('file', '-') # for AlertKafka

    def run(self):
        logger.debug("JsonIn.run()")
        try:
            with fileinput.input(files=self.filenames) as f:
                for line in f:
                    key, value, t = json.loads(line)
                    key = bytes(key, 'ascii')
                    yield (key, value, t)
        finally:
            logger.debug("JsonIn.run() finally")

        logger.debug("JsonIn.run() done")
