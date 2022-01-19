"""Filter that converts 64-bit unsigned values to signed.

Configuration parameters: none

Input:  (key, value, time)

Output:  (key, value, time)
    key is the same as input key.
    value is the input value converted from unsigned to signed.
    time is the same as input time.
"""

import logging
from .. import SentryModule

logger = logging.getLogger(__name__)

class ToSigned(SentryModule.SentryModule):
    def __init__(self, config, gen, ctx):
        logger.debug("ToSigned.__init__")
        super().__init__(config, logger, gen)

    @staticmethod
    def unsignedToSignedFactory(bitlength):
        negativeBits = (-1 << (bitlength - 1))
        def f(number):
            if number is None:
                return None
            if number & negativeBits:    # if lowest negative bit is on
                number |= negativeBits   # turn them all on (sign extension)
            return number
        return f

    def run(self):
        logger.debug("ToSigned.run()")
        u_to_s_64 = self.unsignedToSignedFactory(64)
        for entry in self.gen():
            logger.debug("TS: %s", str(entry))
            key, value, t = entry
            value = u_to_s_64(value)
            yield (key, value, t)
