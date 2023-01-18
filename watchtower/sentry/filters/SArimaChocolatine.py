import logging
from libchocolatine.libchocolatine import ChocolatineDetector
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
        self.detector.start()

    def run(self):
        logger.debug("SArimaChocolatine.run()")
        for entry in self.gen():
            key, value, t = entry
            if value is None:
                continue
            self.detector.queueLiveData(key, t, value)
            
