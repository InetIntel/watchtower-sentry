import logging, time
from libchocolatine.libchocolatine import ChocolatineDetector
from libchocolatine.asyncfetcher import AsyncHistoryFetcher
from .. import SentryModule
from iterators import TimeoutIterator

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
        "detectors": {"type": "integer", "exclusiveMinimum": 0},
        "maxarma": {"type": "integer", "exclusiveMinimum": 0},
        "minpredict": {"type": "integer", "inclusiveMinimum": 0},
    },
    "required": ["name"],
}

class SArimaChocolatine(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("SArimaChocolatine.__init__")
        super().__init__(config, logger, gen)
        self.config = config

        self.maxarma = config.get("maxarma", 3)
        self.minpredict = config.get("minpredict", 1)
        self.name = config.get("name", "unknown")
        self.dbconf = config.get("dbconf", {})
        self.kafkaconf = {}
        self.kafkaconf['modellertopic'] = "chocolatine.model"
        self.kafkaconf['bootstrap-model'] = "capri.cc.gatech.edu:9092"
        self.kafkaconf['group'] = "ioda-watchtower-chocolatine-%s" % (self.name)

        self.numdetectors = config.get("detectors", 8)
        self.activealerts = {}

        if "kafkaconf" in config:
            if 'modellerTopic' in config['kafkaconf']:
                self.kafkaconf['modellertopic'] = config['kafkaconf']['modellerTopic']
            if 'bootstrapModel' in config['kafkaconf']:
                self.kafkaconf['bootstrap-model'] = config['kafkaconf']['bootstrapModel']
            if 'group' in config['kafkaconf']:
                self.kafkaconf['group'] = config['kafkaconf']['group']

        ctx['method'] = "sarima" # for AlertKafka

        self.detectors = []
        self.fetchers = []

    def startDetectorPool(self):

        for i in range(0, self.numdetectors):
            name = "%s-%3d" % (self.name, i)
            self.detectors.append(ChocolatineDetector(name, IODAAPI,
                    self.kafkaconf, self.dbconf, self.maxarma))
            self.fetchers.append(AsyncHistoryFetcher(IODAAPI,
                    self.detectors[i].histRequest,
                    self.detectors[i].histReply))

            self.fetchers[i].start()
            self.detectors[i].start()

    def haltDetectorPool(self):
        for d in self.detectors:
            d.halt()
        for f in self.fetchers:
            f.halt()
        self.detectors = []
        self.fetchers = []

    def getResults(self, key, t):
        for d in self.detectors:
            while True:
                ev = d.getLiveDataResult(False)
                if ev is None:
                    break

                self.queued -= 1
                assert(self.queued >= 0)
                if ev[2] is not None:
                    val = 1.0
                    # Ignore any "events" where the normal time series
                    # is below an accepted minimum value (e.g. regions
                    # where the normal metric value is close to zero).
                    if int(ev[2]['predicted']) < self.minpredict:
                        val = 1.0
                    elif ev[2]['alertable']:
                        # alertable == True
                        # This case means that the observation is below the
                        # SARIMA anomaly threshold.
                        # Move into an event state if we aren't already, and
                        # start using the higher "normal" threshold to
                        # set the bar higher for returning to a non-event state
                        if ev[0] not in self.activealerts:
                            if ev[2]['threshold'] > 0:
                                val = 1 / ((ev[2]['threshold'] - ev[2]['observed']) / ev[2]['baseline'])
                                self.activealerts[ev[0]] = val
                        elif ev[2]['norm_threshold'] > 0:
                            if ev[2]['observed'] > ev[2]['norm_threshold']:
                                val = 1.0
                                del(self.activealerts[ev[0]])
                            else:
                                val = 1 / ((ev[2]['norm_threshold'] - ev[2]['observed']) / ev[2]['baseline'])
                                #self.activealerts[ev[0]] = min(val, self.activealerts[ev[0]])
                                #val = self.activealerts[ev[0]]
                    else:
                        # alertable == False
                        # The observation is above the SARIMA anomaly
                        # threshold, but that doesn't mean that we are ready
                        # to exit an event state -- only exit if the
                        # observation is close to the higher "normal" threshold
                        # as well.
                        if ev[0] in self.activealerts:
                            if ev[2]['norm_threshold'] > 0:
                                if ev[2]['observed'] >= ev[2]['norm_threshold']:
                                    val = 1.0
                                    del(self.activealerts[ev[0]])
                                else:
                                    val = 1 / ((ev[2]['norm_threshold'] - ev[2]['observed']) / ev[2]['baseline'])
                                    #self.activealerts[ev[0]] = min(val, self.activealerts[ev[0]])
                                    #val = self.activealerts[ev[0]]
                        else:
                            # we're not in an event state so we don't have to
                            # be as strict -- if SARIMA is ok with it then
                            # we can be too...
                            val = 1.0

                    actual = int(ev[2]['observed'])
                    pred = int(ev[2]['predicted'])
                    if val > 1.0:
                        val = 1.0

                    #print((ev[0], (val, actual, pred), ev[1]))
                    yield((ev[0], (val, actual, pred), ev[1]))

                    if ev[0] == key and ev[1] == t:
                        break

    def run(self):
        logger.debug("SArimaChocolatine.run()")
        self.startDetectorPool()
        self.queued = 0

        it = TimeoutIterator(self.gen(), timeout=0.5)
        for entry in it:
            if entry != it.get_sentinel():
                key, value, t = entry
                if value is None:
                    continue
                detid = hash(key) % self.numdetectors
                self.detectors[detid].queueLiveData(key, t, value)
                self.queued += 1

                cnt = 0
                for yieldable in self.getResults(key, t):
                    yield yieldable
                    cnt += 1
                    if cnt >= 200:
                        break


        while self.queued > 0:
            for yieldable in self.getResults(None, None):
                yield yieldable

        self.haltDetectorPool()
