# hermes-traverser

Python script that walks around [Hermes](https://github.com/allegro/hermes) Zookeeper and changes stuff.

## Install

In `hermes-traverser` directory do:

```bash
virtualenv venv
. venv/bin/activate
pip3 install -r requirements.txt
```

## hermes-traverser.py

```bash
python hermes-traverser.py --help
python hermes-traverser.py -z my-hermes-zookeeper.host:2181 -p /run/hermes --dryrun
```

### Effects

By default `hermes-traverser` fixes subscriptions without `SupportTeam` field: `undefined` value is set.

### Customization

Change the Python code to change whatever in either Hermes topics or subscriptions.

## hermes-owner-migrator.py

Batch changes of topic and subscription ownership in Hermes. Reads data from CSV file and applies changes to Hermes.

### CSV file format:

```
Topic,Subscription,Owner Source,Owner ID
```

`Subscription` is optional - there can be no such column or value can be empty. If `Subscription` is empty, row describes
a topic. If not, row describes `Subscription`.

If topic/subscription already has given owner source && owner id, no changes are made.

### Usage

```
bash
python hermes-traverser.py --help
python hermes-traverser.py -z my-hermes-zookeeper.host:2181 -p /run/hermes -s data.csv --dryrun
```