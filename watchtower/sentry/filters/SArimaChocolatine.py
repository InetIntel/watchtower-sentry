import logging, time
from libchocolatine.libchocolatine import ChocolatineDetector
from libchocolatine.asyncfetcher import AsyncHistoryFetcher
from .. import SentryModule

IODAAPI="https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw"

logger = logging.getLogger(__name__)
add_cfg_schema = {
    "properties": {
        "name": {"type": "string"},
        "kafkaconf": {
            "type": "object",
            "properties": {
                "modellerTopic": {"type": "string"},
                "bootstrapModel": {"type": "string"},
                "group": {"type": "string"},
            },
        },
        "dbconf": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "host": {"type": "string"},
                "port": {"type": "integer"},
            },
        },
        "maxarma": {"type": "integer", "exclusiveMinimum": 0},
    },
    "required": ["name"],
}

class SArimaChocolatine(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("SArimaChocolatine.__init__")
        super().__init__(config, logger, gen)
        self.config = config

        self.maxarma = config.get("maxarma", 3)
        self.name = config.get("name", "unknown")
        self.dbconf = config.get("dbconf", {})
        self.kafkaconf = {}
        self.kafkaconf['modellertopic'] = "chocolatine.model"
        self.kafkaconf['bootstrap-model'] = "capri.cc.gatech.edu:9092"
        self.kafkaconf['group'] = "ioda-watchtower-chocolatine-%s" % (self.name)

        if "kafkaconf" in config:
            if 'modellerTopic' in config['kafkaconf']:
                self.kafkaconf['modellertopic'] = config['kafkaconf']['modellerTopic']
            if 'bootstrapModel' in config['kafkaconf']:
                self.kafkaconf['bootstrap-model'] = config['kafkaconf']['bootstrapModel']
            if 'group' in config['kafkaconf']:
                self.kafkaconf['group'] = config['kafkaconf']['group']

        ctx['method'] = "sarima" # for AlertKafka


        self.detector = ChocolatineDetector(self.name, IODAAPI, self.kafkaconf,
                self.dbconf, self.maxarma)
        self.fetcher = AsyncHistoryFetcher(IODAAPI, self.detector.histRequest,
                self.detector.histReply)

        self.fetcher.start()
        self.detector.start()

    def run(self):
        logger.debug("SArimaChocolatine.run()")
        for entry in self.gen():
            key, value, t = entry
            if value is None:
                continue
            self.detector.queueLiveData(key, t, value)

            while True:
                ev = self.detector.getLiveDataResult(False)
                if ev is None:
                    time.sleep(0.1)
                    continue
                if ev[2] is not None:
                    if ev[2]['alertable'] and ev[2]['threshold'] > 0:
                        val = ev[2]['observed'] / ev[2]['threshold']
                    else:
                        val = 1.0

                    actual = int(ev[2]['observed'])
                    pred = int(ev[2]['predicted'])
                    yield((ev[0], (val, actual, pred), ev[1]))


                if ev[0] == key and ev[1] == t:
                    break

        self.detector.halt()
        self.fetcher.halt()
