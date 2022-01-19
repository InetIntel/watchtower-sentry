"""Provides base class and common methods for threaded source modules."""

import sys
import logging
import threading
import time
from .. import SentryModule

logger = logging.getLogger(__name__)


class Datasource(SentryModule.Source):
    def __init__(self, config, modlogger, gen, ctx):
        logger.debug("Datasource.__init__")
        super().__init__(config, modlogger, gen)
        self.done = False
        self.incoming = []
        self.producable = True
        self.consumable = False
        self.reader_exc = None
        # The reader thread produces data by reading it from its source and
        # appending it to self.incoming.
        self.reader = threading.Thread(target=self.reader_thread,
            daemon=True, # program need not join() this thread to exit
            name="DS.reader")
        lock = threading.Lock()
        self.cond_producable = threading.Condition(lock)
        self.cond_consumable = threading.Condition(lock)

    def reader_thread(self):
        try:
            self.reader_body()
            if not self.done:
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (done=True)")
                    self.done = True
                    self.cond_consumable.notify()
        except:
            e = sys.exc_info()[1]
            logger.error(e)
            with self.cond_consumable:
                logger.debug("cond_consumable.notify (exception)")
                self.reader_exc = e
                self.done = True
                self.cond_consumable.notify()


    def reader_body(self):
        raise NotImplementedError() # abstract method

    # Consume data produced by the reader thread, and yield it.
    # May throw exceptions, including those raised in the reader thread.
    def run(self):
        logger.debug("Datasource.run()")
        self.reader.start()
        try:
            while True:
                # wait for reader thread to fill self.incoming
                data = None
                with self.cond_consumable:
                    logger.debug("cond_consumable check")
                    while not self.consumable and not self.done:
                        logger.debug("cond_consumable.wait")
                        self.cond_consumable.wait()
                    if self.consumable:
                        data = self.incoming
                        self.incoming = None
                    elif self.reader_exc:
                        logger.debug("Datasource.run: exception in reader "
                            "thread")
                        raise self.reader_exc
                    else: # if self.done:
                        logger.debug("Datasource.run: end-of-stream")
                        break
                    self.consumable = False
                    logger.debug("cond_consumable.wait DONE (%d items)",
                        len(data))
                # Tell reader thread that self.incoming is ready to be refilled
                with self.cond_producable:
                    logger.debug("cond_producable.notify")
                    self.producable = True
                    self.cond_producable.notify()
                # Give up control to the reader so it can request the next set
                # of data; then while it waits for the response it will return
                # control to this thread.
                time.sleep(0)
                # Process the data.
                for entry in data:
                    yield entry
        finally:
            logger.debug("_Datasource.run() finally")
            if not self.done:
                # notify producer that we're stopping early
                with self.cond_producable:
                    logger.debug("cond_producable.notify (done=True)")
                    self.done = True
                    self.cond_producable.notify()
            logger.debug("joining reader thread")
            self.reader.join()
