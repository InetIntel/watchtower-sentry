import sys
import os
# import signal
import re
import logging
import logging.handlers
import traceback
import argparse
import importlib
import yaml
from . import SentryModule as SM

exitstatus = 0
COMMENT_RE = re.compile(r'//\s+.*$', re.M)
logger = logging.getLogger('watchtower.sentry') # sentry logger


cfg_schema = {
    "title": "Watchtower-Sentry configuration schema",
    "type": "object",
    "properties": {
        "loglevel": {"type": "string"},                # global loglevel
        "pipeline": {                                  # list of modules
            "type": "array",
            "items": SM.base_cfg_schema(),             # generic module
            "minItems": 2,
        }
    },
    "additionalProperties": False
}


class Sentry:

    def __init__(self, options, config = None):
        if config:
            cfg_name = '<config data>'
            self.config = config
        else:
            cfg_name = os.path.abspath(options.configfile)
            if cfg_name:
                self._load_config(cfg_name)

        if 'loglevel' in self.config:
            logging.getLogger().setLevel(self.config['loglevel'])

        pipeline = []
        ctx = dict() # context shared by all modules
        cfg_schema['properties']['pipeline']['items'] = []
        for i, modconfig in enumerate(self.config['pipeline']):
            modname = modconfig['module']
            # load the module
            try:
                pymod = importlib.import_module(name=(".%s" % modname),
                                                package="watchtower.sentry")
            except ModuleNotFoundError as e:
                raise SM.UserError('pipeline[%d]: %s' % (i, str(e)))
            # get the module's class
            classname = modname.rsplit(".", 1)[-1]
            try:
                pyclass = getattr(pymod, classname)
                if not issubclass(pyclass, SM.SentryModule):
                    raise TypeError()
            except (AttributeError, TypeError) as e:
                raise SM.UserError('pipeline[%d]: %s is not a SentryModule' %
                    (i, modname))

            for cls, loc, idx in [(SM.Source, 'first', 0),
                    (SM.Sink, 'last', len(self.config['pipeline']) - 1)]:
                if issubclass(pyclass, cls) != (i == idx):
                    sign = " not" if (i == idx) else ""
                    raise SM.UserError('pipeline[%d]: %s is%s a %s; '
                        'it must%s be %s in pipeline' %
                        (i, modname, sign, cls.__name__, sign, loc))

            pipeline.append({'modconfig': modconfig, 'pyclass': pyclass})
            # Merge module-specific configuration schema with the base
            # module schema, and add the result to the main cfg_schema.
            mod_schema = SM.base_cfg_schema()
            mod_schema['properties']['module'] = {'const': modname}
            mod_schema['additionalProperties'] = False
            try:
                add_cfg_schema = getattr(pymod, 'add_cfg_schema')
            except AttributeError:
                add_cfg_schema = None
            if add_cfg_schema:
                for key, value in add_cfg_schema.items():
                    if key == 'properties':
                        mod_schema['properties'].update(value)
                    elif key == 'required':
                        mod_schema['required'] += value
                    elif key in mod_schema:
                        raise RuntimeError('%s attempted to modify "%s" '
                            'attribute of module schema' % (modname, key))
                    else:
                        mod_schema[key] = value
            cfg_schema['properties']['pipeline']['items'].append(mod_schema)
            # Checking the mod_schema by itself gives much more readable error
            # messages than checking the full schema.
            SM.validator().check_schema(mod_schema)

        # Validate config against the full schema we just built.
        SM.schema_validate(self.config, cfg_schema, cfg_name, logger)

        # Construct instances of each class and chain them together.
        self.run_last_mod = None
        for i, item in enumerate(pipeline):
            mod = item['pyclass'](item['modconfig'], self.run_last_mod, ctx)
            self.run_last_mod = mod.run


    def _load_config(self, filename):
        logger.info('Load configuration: %s', filename)

        try:
            with open(filename) as f:
                source = COMMENT_RE.sub("", f.read())
                self.config = yaml.safe_load(source)
        except Exception as e:
            raise SM.UserError('Invalid config file %s\n%s' %
                (filename, str(e))) from None

        SM.schema_validate(self.config, cfg_schema, filename, logger)

    def run(self):
        logger.debug("sentry.run()")
        self.run_last_mod()
        logger.debug("sentry done")

# end class Sentry


def main(options):
    logger.debug("main()")

#    signal.signal(signal.SIGTERM, s.stop)
#    signal.signal(signal.SIGINT, s.stop)
#    if hasattr(signal, 'SIGHUP'):
#        signal.signal(signal.SIGHUP, s.reinit)

    s = Sentry(options)

    s.run()
    logger.debug("main() done")
    return 0


def cli():
    default_log_level = 'INFO'
    parser = argparse.ArgumentParser(description=
        "Detect outages in IODA data and send alerts to watchtower-alert.")
    parser.add_argument("-c", "--configfile",
        help="name of configuration file",
        required=True)
    parser.add_argument("-L", "--loglevel",
        help=("logging level [%s]" % default_log_level),
        default=default_log_level)
    parser.add_argument("--debug-glob",
        help=("convert a glob to a regex"))
    cmdline_options = parser.parse_args()

    loghandler = logging.StreamHandler()
    loghandler.setFormatter(logging.Formatter(
        '%(asctime)s.%(msecs)03d '
        '%(name)-20s '
        '%(process)d:'
        '%(threadName)-10s '
        '%(levelname)-8s: %(message)s',
        '%H:%M:%S'))
    loglevel = cmdline_options.loglevel
    rootlogger = logging.getLogger()
    rootlogger.addHandler(loghandler)
    rootlogger.setLevel(loglevel)

    # Main body logs all exceptions
    try:
        if cmdline_options.debug_glob:
            print(SM.glob_to_regex(cmdline_options.debug_glob))
            exitstatus = 0
        else:
            exitstatus = main(cmdline_options)
        # print("timestr: %d" % strtimegm(sys.argv[1]))
    except SM.UserError as e:
        logger.critical(str(e))
        exitstatus = 1
    except:
        # possible programming error; include traceback
        e = sys.exc_info()[1]
        logger.critical("%s:\n%s", type(e).__name__, traceback.format_exc())
        exitstatus = 255

    logger.debug('__main__ done')
    sys.exit(exitstatus)


if __name__ == '__main__':
    cli()