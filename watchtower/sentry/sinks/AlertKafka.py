"""Sink that detects extreme values and sends alert objects to a kafka cluster.

Configuration parameters ('*' indicates required parameter):
    fqid*: (string) Unique identifier for this data source.
    name*: (string) Human-readable name for this data source.
    min: (number <1.0) Generate alert if value falls below this value.
    max: (number >1.0) Generate alert if value rises above this value.
    minduraton: (number) Only generate alerts for events at least this long.
    brokers*: (string) Comma-separated list of kafka brokers.
    topic*: (string) Kafka topic prefix.

    At least one of {min} or {max} is required.

Input context variables: expression*, method*

Input:  (key, value, time)

Sink result:  alert objects sent to kafka cluster.
"""

import json
import logging
import confluent_kafka
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "fqid":        {"type": "string"},
        "name":        {"type": "string"},
        "min":         {"type": "number", "exclusiveMaximum": 1.0},
        "max": {"type": "number", "exclusiveMinimum": 1.0},
        "minduration": {"type": "number"},
        "brokers":     {"type": "string"},
        "topic": {"type": "string"},
        "disable":     {"type": "boolean"}, # for debugging
    },
    "required": ["fqid", "name", "brokers", "topic"],
    "oneOf": [{"required": ["min"]}, {"required": ["max"]}]
}

STATUS_NORMAL = 0
STATUS_HIGH = 1
STATUS_LOW = -1


class AlertKafka(SentryModule.Sink):
    def __init__(self, config, gen, ctx):
        logger.debug("AlertKafka.__init__")
        super().__init__(config, logger, gen)
        self.fqid = config['fqid']
        self.name = config['name']
        self.brokers = config['brokers']
        self.topic = config['topic']
        self.min = config.get('min', None)
        self.max = config.get('max', None)
        self.minduration = config.get('minduration', None)
        self.disable = config.get('disable', False)
        self.condition_label = [
            "< %r" % self.min,   # -1
            "normal",            # 0
            "> %r" % self.max    # 1
        ]

        self.alert_status = dict()  # alert_status[key] = [0,1,-1]
        self.alert_state = dict()  # alert_state[key] = (time, value, actual, predicted)
        kp_cfg = {
            'bootstrap.servers': self.brokers,
        }
        self.kproducer = confluent_kafka.Producer(kp_cfg)
        try:
            self.method = ctx['method']
        except KeyError as e:
            raise RuntimeError('%s expects ctx[%s] to be set by a previous '
                'module' % (self.modname, str(e)))

    def _produce_alert(self, status, t, key, value, actual, predicted):
        # Cram our alert data into the watchtower-alert legacy format
        record = {
            "fqid": self.fqid,
            "name": self.name,
            "level": "critical" if status != 0 else "normal",
            "time": t,
            "expression": None,
            "history_expression": None,
            "method": self.method,
            "violations": [{
                "expression": str(key, 'ascii'),
                "condition": self.condition_label[status + 1],
                "value": value if actual is None else actual,
                "history_value": predicted,  # may be None
                "history": None,
                "time": t,
            }],
        }

        # Asynchonously produce a message.  The delivery report
        # callback will be triggered from poll() above, or flush()
        # below, when the message has been successfully delivered or
        # failed permanently.
        msg = json.dumps(record, separators=(',', ':'))
        if self.disable:
            print(msg)
        else:
            self.kproducer.produce(self.topic,
                                   value=bytes(msg, 'ascii'),
                                   key=key,
                                   on_delivery=self.kp_delivery_report)

    def run(self):
        logger.debug("AlertKafka.run()")
        for entry in self.gen():
            logger.debug("AK: %s", str(entry))
            key, value, t = entry

            # Trigger any available delivery report callacks from previous
            # produce() calls
            self.kproducer.poll(0)

            if isinstance(value, tuple):
                (value, actual, predicted) = value
            else:
                actual = None
                predicted = None

            if value is None:
                continue

            if key not in self.alert_status:
                # default to normal status
                self.alert_status[key] = STATUS_NORMAL
            if self.min is not None and value < self.min:
                # "too-low" alert
                alert_status = STATUS_LOW
            elif self.max is not None and value > self.max:
                # "too-high" alert
                alert_status = STATUS_HIGH
            else:
                # "normal" alert
                alert_status = STATUS_NORMAL

            # XXX: following can probably be refactored
            if alert_status != self.alert_status[key]:
                # change in status, either trigger an alert or defer and keep
                # state
                self.alert_status[key] = alert_status

                if self.minduration is None or self.minduration == 0:
                    # minduration is disabled, so trigger alert now
                    self._produce_alert(alert_status, t, key, value,
                                        actual, predicted)
                elif alert_status == STATUS_NORMAL:
                    # back to normal
                    if key not in self.alert_state:
                        # back to normal, but we have a minduration set, so we
                        # would have tracked state for this event. given that
                        # there is no state, it means the event was long enough
                        # to trigger an alert, so we need to trigger the
                        # normal event
                        logger.info("Creating normal alert for %s at %d" %
                                    (key, t))
                        self._produce_alert(alert_status, t, key, value,
                                            actual, predicted)
                    else:
                        # back to normal, and we have a minduration, so given
                        # that there is state being tracked, we haven't yet
                        # reached the minduration, so the outage must have been
                        # too short, just clean up state
                        (init_t, init_v, init_a, init_p) = self.alert_state[key]
                        logger.info("Discarding suppressed alert for '%s' "
                                    "(init_t: %d, t: %d, minduration: %d)"
                                    % (key, init_t, t, self.minduration))
                        if (t - init_t) > self.minduration:
                            logger.warning("Discarding suppressed alert for "
                                           "'%s' that exceeds minduration "
                                           "(init_t: %d, t: %d, minduration: %d)"
                                           % (key, init_t, t, self.minduration))
                        del self.alert_state[key]
                else:
                    # we have a minduration, and this is an "outage" event,
                    # start tracking state
                    self.alert_state[key] = (t, value, actual, predicted)
                    logger.info("Suppressing alert for %s" % key)
            elif alert_status != STATUS_NORMAL:
                # continuation of the event (but not continuation of normal)
                if key in self.alert_state:
                    # we're tracking state about this event, so we haven't
                    # yet triggered the alert. check the duration and maybe
                    # trigger the alert
                    (init_t, init_v, init_a, init_p) = self.alert_state[key]
                    if (init_t + self.minduration) <= t:
                        logger.info("Suppressed alert for '%s' passed minduration "
                                    "(init_t: %d, t: %d, minduration: %d)" %
                                    (key, init_t, t, self.minduration))
                        self._produce_alert(alert_status, init_t, key, init_v,
                                            init_a, init_p)
                        del self.alert_state[key]
                    else:
                        logger.info("Continuing to suppress alert for %s "
                                    "(duration: %d)" % (key, t - init_t))
            else:
                # continuation of normal, who cares
                pass

        self.kproducer.flush()
        logger.debug("AlertKafka.run() done")

    def kp_delivery_report(self, err, msg):
        if err is not None:
            logger.error("message delivery failed: %r", err)
        else:
            logger.debug("message delivered to %r [%r]",
                msg.topic(), msg.partition())
