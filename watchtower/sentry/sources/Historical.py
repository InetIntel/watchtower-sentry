"""Source that reads historic (k,v,t) tuples from the IODA API.

Configuration parameters ('*' indicates required parameter):
    expression*: (string) A DBATS-style glob pattern that input keys must
        match.
    starttime*: (string) Fetch data at or after this time.
        Format: 'YYYY-mm-dd [HH:MM[:SS]]'.
    endtime*: (string) Fetch data before this time.
        Format: 'YYYY-mm-dd [HH:MM[:SS]]'.
    url*: (string) IODA HTTP API URL
    batchduration*: (integer) How much data (in seconds) should be retrieved
        with each call to the API.
    ignorenull: (boolean, default false) If true, null values will be skipped.
        If false, null values will be treated as 0.
    queryparams: (object) Dictionary of extra parameters to pass to the API.

Output context variables: expression

Output:  (key, value, time)
"""

import logging
import requests
from .. import SentryModule
from ._Datasource import Datasource

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expression":    {"type": "string"},
        "starttime":     {"type": "string"},
        "endtime":       {"type": "string"},
        "url":           {"type": "string"},
        "batchduration": {"type": "integer", "exclusiveMinimum": 0},
        "ignorenull":    {"type": "boolean"},
        "queryparams":   {"type": "object"},
    },
    "required": ["expression", "starttime", "endtime", "url", "batchduration"]
}

class Historical(Datasource):

    def __init__(self, config, gen, ctx):
        logger.debug("Historical.__init__")
        super().__init__(config, logger, gen, ctx)
        self.loop = None
        self.client = None
        self.expression = config['expression']
        self.start_time = SentryModule.strtimegm(config['starttime'])
        self.end_time = SentryModule.strtimegm(config['endtime'])
        self.batch_duration = config['batchduration']
        self.queryparams = config.get('queryparams', None)
        self.end_batch = self.start_time
        self.url = config['url']
        self.request = None

    def make_next_request(self):
        start_batch = self.end_batch
        if start_batch >= self.end_time:
            return False
        self.end_batch += self.batch_duration
        if self.end_batch >= self.end_time:
            self.end_batch = self.end_time
        post_data = {
            'from': start_batch,
            'until': self.end_batch,
            'expression': self.expression
        }
        if self.queryparams:
            post_data.update(self.queryparams)
        logger.debug("request: %d - %d", start_batch, self.end_batch)
        self.request = requests.post(self.url, data=post_data, timeout=60)
        return True

    def handle_response(self):
        logger.debug("response code: %d", self.request.status_code)
        self.request.raise_for_status()
        result = self.request.json()
        logger.debug("response: %s - %s", result['queryParameters']['from'],
            result['queryParameters']['until'])

        # wait for self.incoming to be empty
        with self.cond_producable:
            logger.debug("cond_producable check")
            while not self.producable and not self.done:
                logger.debug("cond_producable.wait")
                self.cond_producable.wait()
            if self.done:
                return
            self.incoming = []
            self.producable = False
            logger.debug("cond_producable.wait DONE")
        series = result['data']['series']
        for key, record in series.items():
            t = int(record['from'])
            step = int(record['step'])
            ascii_key = bytes(key, 'ascii')
            for value in record['values']:
                self.incoming.append((ascii_key, value, t))
                t += step
        # tell computation thread that self.incoming is now full
        with self.cond_consumable:
            logger.debug("cond_consumable.notify")
            self.consumable = True
            self.cond_consumable.notify()

    def reader_body(self):
        logger.debug("historic.run_reader()")
        try:
            while not self.done and self.make_next_request():
                self.handle_response()
        except requests.ConnectionError as e:
            # strip excess information about guts of requests module
            raise ConnectionError(str(e)) from None
        logger.debug("historic done")
