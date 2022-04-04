# Portions of this source code are Copyright (c) 2021 Georgia Tech Research
# Corporation. All Rights Reserved. Permission to copy, modify, and distribute
# this software and its documentation for academic research and education
# purposes, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice, this paragraph and the following
# three paragraphs appear in all copies. Permission to make use of this
# software for other than academic research and education purposes may be
# obtained by contacting:
#
#  Office of Technology Licensing
#  Georgia Institute of Technology
#  926 Dalney Street, NW
#  Atlanta, GA 30318
#  404.385.8066
#  techlicensing@gtrc.gatech.edu
#
# This software program and documentation are copyrighted by Georgia Tech
# Research Corporation (GTRC). The software program and documentation are 
# supplied "as is", without any accompanying services from GTRC. GTRC does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL GEORGIA TECH RESEARCH CORPORATION BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF GEORGIA TECH RESEARCH CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE. GEORGIA TECH RESEARCH CORPORATION SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND  GEORGIA TECH RESEARCH CORPORATION HAS
# NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.

"""Source that reads (k,v,t) tuples from a live TSK (Time Series Kafka) service.

Configuration parameters ('*' indicates required parameter):
    expression*: (string) A DBATS-style glob pattern that input keys must
        match.
    brokers*: (string) Comma-separated list of kafka brokers.
    consumergroup*: (string) Kafka consumer group.
    topicprefix*: (string) Kafka topic prefix.
    channelname*: (string) Kafka channel name.

Output context variables: expression

Output:  (key, value, time)
   Output will include some amount (perhaps several days worth) of buffered
   data prior to the near-realtime data.
"""

import confluent_kafka
import logging
import re
import time
import sys
from .. import SentryModule
from ._Datasource import Datasource


# list of kafka "errors" that are not really errors
KAFKA_IGNORED_ERRS = [
    confluent_kafka.KafkaError._PARTITION_EOF,
    confluent_kafka.KafkaError._TIMED_OUT,
]
logger = logging.getLogger(__name__)

class GraphiteKafkaReader:
    def __init__(self, topic_prefix, channel, consumer_group, brokers,
                partition=None, reset_offsets=False, commit_offsets=True):

        self.channel = bytes(channel, 'ascii')
        self.topic_name = ".".join([topic_prefix, channel])
        self.consumer_group = ".".join([consumer_group, self.topic_name])
        self.partition = partition

        self.conf = {
            'bootstrap.servers': brokers,
            'group.id': self.consumer_group,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 3000,
            'api.version.request': True,
            'enable.auto.commit': commit_offsets
        }

        self.kc = None

        if reset_offsets:
            logging.info("Resetting commited offsets")
            raise NotImplementedError

    def connect(self):
        if self.kc is not None:
             logging.warning("GraphiteKafkaReader.connect() called on an already connected instance?")
             return

        self.kc = confluent_kafka.Consumer(self.conf)
        if self.partition:
            topic_list = [confluent_kafka.TopicPartition(self.topic_name,
                        self.partition)]
            self.kc.assign(topic_list)
        else:
            self.kc.subscribe([self.topic_name])

    def close(self):
        return self.kc.close()

    def poll(self, time):
        return self.kc.poll(time)

    def handle_msg(self, msgbuf, msg_cb, kv_cb):
        msg_ts = 0

        kvs = msgbuf.split(b'\n')
        for kv in kvs:
            msg = kv.decode().split(" ")
            if len(msg) != 3:
                logging.debug("Unexpected message format: %s" % (kv.decode()))
                continue
            timestamp = int(msg[2])
            if msg_ts == 0:
                msg_cb(timestamp, 1, self.channel, kv, len(kv))
                msg_ts = timestamp
            else:
                assert(msg_ts == timestamp)

            key = msg[0]
            val = int(msg[1])

            kv_cb(key, val)


add_cfg_schema = {
    "properties": {
        "expressions": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1
        },
        "brokers": {"type": "string"},
        "consumergroup": {"type": "string"},
        "topicprefix":   {"type": "string"},
        "channelname":   {"type": "string"},
    },
    "required": ["expressions", "brokers", "consumergroup",
                 "topicprefix", "channelname"]
}

class Realtime(Datasource):

    def __init__(self, config, gen, ctx):
        logger.debug("Realtime.__init__")
        super().__init__(config, logger, gen, ctx)
        self.expressions = config['expressions']
        self.tsk_reader = GraphiteKafkaReader(
                config['topicprefix'],
                config['channelname'],
                config['consumergroup'],
                config['brokers'],
                commit_offsets=True
        )
        self.msg_time = None
        regexes = [SentryModule.glob_to_regex(exp) for exp in self.expressions]
        logger.debug("expressions: %s", self.expressions)
        logger.debug("regexes:     %s", regexes)
        self.expression_res = [re.compile(regex) for regex in regexes]
        self.kv_cnt = 0
        self.kv_match_cnt = 0

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        self.msg_time = msg_time

    def _kv_cb(self, key, val):
        self.kv_cnt += 1
        for regex in self.expression_res:
            if regex.match(key):
                self.kv_match_cnt += 1
                self.incoming.append((key, val, self.msg_time))
                return

    def reader_body(self):
        logger.debug("realtime.run_reader()")
        last_log_time = time.time()
        self.tsk_reader.connect()
        while not self.done:
            now = time.time()
            if last_log_time + 60 <= now:
                logging.info("Realtime: %d KVs (%f per sec.), "
                             "%d matched kvs (%f per sec.)" %
                             (self.kv_cnt, self.kv_cnt/(now-last_log_time),
                              self.kv_match_cnt,
                              self.kv_match_cnt/(now-last_log_time)))
                self.kv_cnt = 0
                self.kv_match_cnt = 0
                last_log_time = now
            logger.debug("tsk_reader_poll")
            msg = self.tsk_reader.poll(10000)
            if msg is None:
                logger.debug("TSK msg: None")
                break
            if not msg.error():
                # wait for self.incoming to be empty
                logger.debug("TSK msg: non-error")
                with self.cond_producable:
                    logger.debug("cond_producable check")
                    while not self.producable and not self.done:
                        logger.debug("cond_producable.wait")
                        self.cond_producable.wait()
                    self.incoming = []
                    self.producable = False
                    logger.debug("cond_producable.wait DONE")
                if self.done: # in case consumer stopped early
                    break
                self.tsk_reader.handle_msg(msg.value(),
                    self._msg_cb, self._kv_cb)
                # tell computation thread that self.incoming is now full
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify")
                    self.consumable = True
                    self.cond_consumable.notify()
            elif msg.error().code() in KAFKA_IGNORED_ERRS:
                logger.debug("Ignoring benign kafka 'error': %s" % msg.error().code())
            else:
                logger.error("Unhandled Kafka error, shutting down")
                logger.error(msg.error())
                with self.cond_consumable:
                    logger.debug("cond_consumable.notify (error)")
                    self.reader_exc = RuntimeError("kafka: %s" % msg.error())
                    self.done = True
                    self.cond_consumable.notify()
                break
        logger.debug("realtime done")
