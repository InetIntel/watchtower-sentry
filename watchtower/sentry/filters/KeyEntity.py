"""Filter that derives entity types and codes from a key.

The key will be replaced with a string of the format "entitytype/code",
which can be used to query the entities/ API to obtain full information
(including FQID) for the entity matching that code.

Configuration parameters ('*' indicates required parameter):
    expressions*: (array) An array of DBATS-style glob patterns that input
        keys must match. Entity types and codes are extracted from the
        substrings that match parenthesized subexpressions.

        Within an expression entry, there are two parameters that
        must be provided:

            pattern*: the regex pattern for an input key
            metatype*: the entity type for series that match this expression

Example config:

   expressions:
      - pattern: "bgp.prefix-visibility.geo.netacuity.*.(*).v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt"
        metatype: "country"

In this example, the term that is matched within the parentheses () will be
deemed to be the country code for the matched series.

      - pattern: 'bgp.prefix-visibility.geo.netacuity.*.*.(*).v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt'
        metatype: "region"

In this example, the term that is matched within the parentheses () will be
the region code for the matched series.


Input: (key, value, time)

Output: (entity, value, time)
"""

import logging
import re
import time
from .. import SentryModule

logger = logging.getLogger(__name__)

add_cfg_schema = {
    "properties": {
        "expressions": {
             "type": "array",
             "items": [{
                   "type": "object",
                   "properties": {
                        "pattern": {"type": "string"},
                        "metatype": {"type": "string"},
                   },
                   "additionalProperties": False,
                   "required": ["pattern", "metatype"],
             }]
        },
    },
    "required": ["expressions"]
}

class KeyEntity(SentryModule.SentryModule):

    def __init__(self, config, gen, ctx):
        logger.debug("KeyEntity.__init__")
        super().__init__(config, logger, gen)
        self.expressions = config['expressions']

        regexes = [SentryModule.glob_to_regex(xp.get('pattern', None)) for xp in self.expressions]
        self.expression_res = [re.compile(r) for r in regexes]

    def run(self):
        logger.debug("KeyEntity.run()")
        for entry in self.gen():
            logger.debug("KE: %s", entry)
            key, value, t = entry
            match = False

            for idx, exp_re in enumerate(self.expression_res):
                match = exp_re.match(key)
                if match:
                     matched_exp = self.expressions[idx]
                     break
            if not match:
                continue

            groups = match.groups()

            try:
                entityfull = "%s/%s" % (matched_exp['metatype'], groups[0])
            except:
                logger.error("Cannot construct entity from key '%s' using expression %s -- %s" % (key, matched_exp, groups))
                continue
            yield(entityfull, value, t)
        logger.debug("KeyEntity.run() done")
