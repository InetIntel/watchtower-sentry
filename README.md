Watchtower Sentry
=================

Time series monitoring and processing pipeline.

See the
[Watchtower-V2](https://github.com/CAIDA/watchtower-sentry/wiki/Watchtower-V2)
wiki page for a detailed description of the Watchtower architecture.

## Install

```
sudo python3 setup.py install
```

## Configuration

Watchtower requires a configuration file. See
[sentry-sample.yaml](/sentry-sample.yaml) for a worked example.

## Running

After installing watchtower-sentry and creating a configuration file, simply
run watchtower like so:
```
watchtower-sentry --configfile=/path/to/config.yaml
```

## License

Watchtower-Sentry is released for academic, non-commerical use. See the full
[LICENSE](/LICENSE) for more information.
