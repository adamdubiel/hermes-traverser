# hermes-traverser

Python script that walks around [Hermes](https://github.com/allegro/hermes) Zookeeper and changes stuff.

## Install

In `hermes-traverser` directory do:

```bash
virtualenv .
. bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python hermes-traverser.py --help
python hermes-traverser.py -z my-hermes-zookeeper.host:2181 -p /run/hermes --dryrun
```

## Effects

By default `hermes-traverser` fixes subscriptions without `SupportTeam` field: `undefined` value is set.

## Customization

Change the Python code to change whatever in either Hermes topics or subscriptions.
