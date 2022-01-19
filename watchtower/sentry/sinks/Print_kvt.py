import logging
from .. import SentryModule

logger = logging.getLogger(__name__)


class Print_kvt(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("Print_kvt.__init__")
        super().__init__(config, logger, gen)

    def run(self):
        logger.debug("Print_kvt.run()")
        for entry in self.gen():
            print(str(entry))
        logger.debug("Print_kvt.run() done")
