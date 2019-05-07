import click
import json
import csv
from kazoo.client import KazooClient

@click.command()
@click.option('--zookeeper', '-z', required = True, help = 'zookeeper connection string')
@click.option('--prefix', '-p', required = True, default = '/run/hermes', help = 'path prefix')
@click.option('--source', '-s', required = True, help = "CSV file to load migration information from: Topic,Subscription,Owner Source,Owner ID")
@click.option('--dryrun', is_flag = True, help = "dry run mode won't modify instances")
def sc_migrator(zookeeper, prefix, source, dryrun):

    """Changes ownership of topics / subscriptions based on CSV file input"""

    click.echo("""
Starting Hermes Ownership Migrator
==================================
    """)

    data = load_csv_configuration(source)
    for k, v in data.topics.items():
        click.echo("{} {}".format(k, v))

    zk = connect_to_zookeeper(zookeeper)
    traverse(zk, prefix, data, dryrun)

def connect_to_zookeeper(connectionString):
    zk = KazooClient(hosts=connectionString)
    zk.start()
    return zk

class Owner:
    def __init__(self, source, id):
        self.source = source
        self.id = id

    def __str__(self):
        return "{{'source': {}, 'id': {}}}".format(self.source, self.id)

class TopicMigrationData:
    def __init__(self, name, owner: Owner):
        self.name = name
        self.owner = owner
    
    def __str__(self):
        return "{{'name': {}, 'owner': {}, 'id': {}}}".format(self.name, self.owner.source, self.owner.id)

class SubscriptionMigrationData:
    def __init__(self, topic_name, name, owner: Owner):
        self.topic_name = topic_name
        self.name = name
        self.owner = owner
    
    def __str__(self):
        return "{{'name': {}${}, 'owner': {} 'id': {}}}".format(self.topic_name, self.name, self.owner.source, self.owner.id)

class TopicAndSubMigrationData:
    def __init__(self, topic: TopicMigrationData):
        self.topic = topic
        self.subscriptions = {}
    
    def add_subscription(self, sub: SubscriptionMigrationData):
        self.subscriptions["{}${}".format(sub.topic_name, sub.name)] = sub
    
    def has_subscriptions(self):
        return len(self.subscriptions) > 0
    
    def subscription(self, fqdn) -> SubscriptionMigrationData:
        return self.subscriptions.get(fqdn, None)

    def __str__(self):
        return "{{'topic': {}, 'subscriptions': {}}}".format(self.topic, [s.__str__() for s in self.subscriptions.values()])

class MigrationData:
    def __init__(self):
        self.topics = {}
    
    def add_topic(self, topic: TopicMigrationData) -> TopicAndSubMigrationData:
        if topic.name not in self.topics:
            self.topics[topic.name] = TopicAndSubMigrationData(topic)
        return self.topics[topic.name]
    
    def add_subscription(self, sub: SubscriptionMigrationData) -> TopicAndSubMigrationData:
        if sub.topic_name not in self.topics:
            self.topics[sub.topic_name] = TopicAndSubMigrationData(None)
        self.topics[sub.topic_name].add_subscription(sub)
        return self.topics[sub.topic_name]
    
    def find_topic(self, topicAndGroup) -> TopicAndSubMigrationData:
        return self.topics.get(topicAndGroup, None)


def load_csv_configuration(source) -> MigrationData:
    migrationData = MigrationData()
    with open(source, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            if bool(row.get('Subscription', '')):
                migrationData.add_subscription(
                    SubscriptionMigrationData(row['Topic'], row['Subscription'], Owner(row['Owner Source'], row['Owner ID']))
                )
            else:
                migrationData.add_topic(
                    TopicMigrationData(row['Topic'], Owner(row['Owner Source'], row['Owner ID']))
                )
    return migrationData

def traverse(zk, prefix, migration_data, dryrun):
    counter = 0
    for group in zk.get_children("{}/groups".format(prefix)):
        for topic in zk.get_children("{}/groups/{}/topics".format(prefix, group)):
            topicWithGroup = "{}.{}".format(group, topic)

            topicAndSub = migration_data.find_topic(topicWithGroup)

            if topicAndSub:
                click.echo("Found topic: {} in CSV data".format(topicWithGroup))

                if topicAndSub.topic:
                    try:
                        data, stat = zk.get("{}/groups/{}/topics/{}".format(prefix, group, topic))
                        topicData = json.loads(data.decode("utf-8"))

                        if 'owner' not in topicData:
                            topicData['owner'] = {'source': 'unknown', 'id': 'unknown'}

                        if topicData['owner']['source'] != topicAndSub.topic.owner.source or topicData['owner']['id'] != topicAndSub.topic.owner.id:

                            click.echo("Changing owner of topic: {} from: {} to: {} {}".format(
                                topicWithGroup, topicData['owner']['id'], topicAndSub.topic.owner.source, topicAndSub.topic.owner.id
                            ))

                            topicData['owner']['source'] = topicAndSub.topic.owner.source
                            topicData['owner']['id'] = topicAndSub.topic.owner.id

                            counter = counter + 1

                            if not dryrun:
                               zk.set(
                                   "{}/groups/{}/topics/{}".format(prefix, group, topic),
                                   bytes(json.dumps(topicData), "utf-8")
                               )
                    except ValueError:
                       click.echo("Unable to read topic data: {}".format(topicWithGroup)) 
                    
                if topicAndSub.has_subscriptions():
                    for sub in zk.get_children("{}/groups/{}/topics/{}/subscriptions/".format(prefix, group, topic)):
                        subscription_fqdn = "{}${}".format(topicWithGroup, sub)
                        subMigrationData = topicAndSub.subscription(subscription_fqdn)

                        if subMigrationData:
                            try:
                                data, stat = zk.get("{}/groups/{}/topics/{}/subscriptions/{}".format(prefix, group, topic, sub))
                                subData = json.loads(data.decode("utf-8"))

                                if 'owner' not in subData:
                                    subData['owner'] = {'source': 'unknown', 'id': 'unknown'}

                                if subData['owner']['source'] != subMigrationData.owner.source or subData['owner']['id'] != subMigrationData.owner.id:
                                    click.echo("Changing owner of sub: {} from: {} to: {} {}".format(
                                        subscription_fqdn, subData['owner']['id'], subMigrationData.owner.source, subMigrationData.owner.id
                                    ))

                                    subData['owner']['source'] = subMigrationData.owner.source
                                    subData['owner']['id'] = subMigrationData.owner.id

                                    counter = counter + 1

                                    if not dryrun:
                                       zk.set(
                                           "{}/groups/{}/topics/{}/subscriptions/{}".format(prefix, group, topic, sub),
                                           bytes(json.dumps(subData), "utf-8")
                                       )
                            except ValueError:
                                click.echo("Unable to read sub data: {}".format(subscription_fqdn))

    click.echo("Changed owner for {} topics & subs".format(counter))

if __name__ == '__main__':
    sc_migrator()
