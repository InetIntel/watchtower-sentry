"""
Base class for Watchtower Sentry modules.

Derived classes can implement a source, a filter, or a sink module.
Derived classes must implement an __init__(self, config, gen, ctx) method that
calls super().__init__(config, logger, gen, ctx).
Sources and sinks must subclass SentryModule.Source or SentryModule.Sink.
Sources and filters must implement run(self) as a python generator function
that yields (key, value, time) tuples.
Filters and sinks must implement run(self) as function that reads
(key, value, time) tuples by iterating over the gen() generator.
The module should do any necessary cleanup in a `finally` clause in run().
"""

import calendar
import time
import jsonschema


def base_cfg_schema():
    return {
        "type": "object",
        "properties": {
            "module":   {"type": "string"}, # module name
            "loglevel": {"type": "string"}, # module loglevel
            # subclass can add more properties in add_cfg_schema
        },
        "required": ["module"], # subclass can add more in add_cfg_schema
        # subclass can add more attributes in add_cfg_schema
    }


class UserError(RuntimeError):
    pass


class SentryModule:
    def __init__(self, config, logger, gen):
        if 'loglevel' in config:
            logger.setLevel(config['loglevel'])
        self.gen = gen
        self.modname = config['module']

class Source(SentryModule):
    pass

class Sink(SentryModule):
    pass

# Convert a time string in 'YYYY-mm-dd [HH:MM[:SS]]' format (in UTC) to a
# unix timestamp
def strtimegm(s):
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return calendar.timegm(time.strptime(s, fmt))
        except ValueError:
            continue
    raise ValueError("Invalid date '%s'; expected 'YYYY-mm-dd [HH:MM[:SS]]'"
        % s)


def validator():
    return jsonschema.Draft7Validator

def schema_validate(instance, schema, name, logger):
    # jsonschema validates the data structure, not the source it was loaded
    # from, so works whether the source was yaml or json or whatever.
    validator().check_schema(schema)
    v = validator()(schema)
    if not v.is_valid(instance):
        logger.error("Errors found in %s:" % name)
        for e in v.iter_errors(instance):
            path = ''.join([('[%d]' % i) if isinstance(i, int) \
                else ('.%s' % str(i)) for i in e.absolute_path])
            logger.error('  %s: %s' % (path[1:], e.message))
        raise UserError('invalid config file %s' % name)


# Convert a DBATS glob to a regex.
# Unlike DBATS, this also allows non-nested parens for aggregate grouping.
#
# From DBATS docs:
# The pattern is similar to shell filename globbing, except that hierarchical
# components are separated by '.' instead of '/'.
#   * matches any zero or more characters (except '.')
#   ? matches any one character (except '.')
#   [...] matches any one character in the character class (except '.')
#       A leading '^' negates the character class
#       Two characters separated by '-' matches any ASCII character between
#           the two characters, inclusive
#   {...} matches any one string of characters in the comma-separated list of
#       strings
#   Any other character matches itself.
#   Any special character can have its special meaning removed by preceeding
#       it with '\'.
def glob_to_regex(glob):
    re_meta = '.^$*+?{}[]|()'
    glob_meta = '*?{}[]()'
    regex = '^'
    i = 0
    parens = 0
    while i < len(glob):
        if glob[i] == '\\':
            i += 1
            if i >= len(glob):
                raise UserError("illegal trailing '\\' in pattern")
            elif glob[i] not in glob_meta:
                raise UserError("illegal escape '\\%s' in pattern" % glob[i])
            elif glob[i] in re_meta:
                regex += '\\'
            regex += glob[i]
            i += 1
        elif glob[i] == '*':
            regex += '[^.]*'
            i += 1
        elif glob[i] == '?':
            regex += '[^.]'
            i += 1
        elif glob[i] == '[':
            regex += '['
            i += 1
            if i < len(glob) and glob[i] == '^':
                regex += '^.'
                i += 1
            while True:
                if i >= len(glob):
                    raise UserError("unmatched '[' in pattern")
                if glob[i] == '\\' and i+1 < len(glob):
                    regex += glob[i:i+2]
                    i += 2
                else:
                    regex += glob[i]
                    i += 1
                    if glob[i-1] == ']':
                        break
        elif glob[i] == '{':
            regex += '(?:' # non-capturing group
            i += 1
            while True:
                if i >= len(glob):
                    raise UserError("unmatched '{' in pattern")
                elif glob[i] == '\\':
                    if i+1 >= len(glob):
                        raise UserError("illegal trailing '\\' in pattern")
                    regex += glob[i:i+2]
                    i += 2
                elif glob[i] == ',':
                    regex += '|'
                    i += 1
                elif glob[i] == '}':
                    regex += ')'
                    i += 1
                    break
                elif glob[i] in '.*{}[]()':
                    raise UserError("illegal character '%s' inside {} in pattern" % glob[i])
                else:
                    if glob[i] in re_meta:
                        regex += '\\'
                    regex += glob[i]
                    i += 1
        elif glob[i] == '(':
            if parens > 0:
                raise UserError("illegal nested parentheses in pattern")
            parens += 1
            regex += '('  # capturing group
            i += 1
        elif glob[i] == ')' and parens:
            parens -= 1
            regex += ')'
            i += 1
        else:
            if glob[i] in re_meta:
                regex += '\\'
            regex += glob[i]
            i += 1
    if parens > 0:
        raise UserError("unmatched '(' in pattern")
    regex += '$'
    return regex
