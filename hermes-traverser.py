import click
import json
from kazoo.client import KazooClient

@click.command()
@click.option('--zookeeper', '-z', required = True, help = 'zookeeper connection string')
@click.option('--prefix', '-p', required = True, default = '/run/hermes', help = 'path prefix')
@click.option('--dryrun', is_flag = True, help = "dry run mode won't modify instances")
def malformedInstancesFixer(zookeeper, prefix, dryrun):

    """Walks around Hermes and looks for stuff"""

    click.echo("""
Starting Hermes traverser
==================================
    """)

    zk = connectToZookeeper(zookeeper)
    traverse(zk, prefix, dryrun)


def connectToZookeeper(connectionString):
    zk = KazooClient(hosts=connectionString)
    zk.start()
    return zk

def traverse(zk, prefix, dryrun):
    for group in zk.get_children("{}/groups".format(prefix)):
        for topic in zk.get_children("{}/groups/{}/topics".format(prefix, group)):
            for subscription in zk.get_children("{}/groups/{}/topics/{}/subscriptions".format(prefix, group, topic)):
                data, stat = zk.get("{}/groups/{}/topics/{}/subscriptions/{}".format(prefix, group, topic, subscription))
                try:
                    subscriptionData = json.loads(data.decode("utf-8"))
                    if 'supportTeam' not in subscriptionData:
                        if dryrun:
                            click.echo("Subscription without supportTeam: {} {}".format(subscription, subscriptionData));
                        else:
                            click.echo("Fixing subscription without supportTeam: {}".format(subscription));
                            subscriptionData['supportTeam'] = 'undefined'
                            zk.set("{}/groups/{}/topics/{}/subscriptions/{}".format(prefix, group, topic, subscription), json.dumps(subscriptionData))
                except ValueError:
                    click.echo("Unable to read sub data: {}.{} {}".format(group, topic, subscription))

if __name__ == '__main__':
    malformedInstancesFixer()
