import json
from random import randint
from time import sleep

import click
from kazoo.client import KazooClient


def connect_to_zookeeper(connection_string):
    zk = KazooClient(hosts=connection_string)
    zk.start()
    return zk


@click.command()
@click.option('--zookeeper', '-z', required=True, help='zookeeper connection string')
@click.option('--prefix', '-p', required=True, default = '/run/hermes', help='path prefix')
@click.option('--save', is_flag=True, help="write changes to zookeeper")
@click.option('--fix-subscriptions', is_flag=True, help="cleanup max-rate tree at subscription level")
@click.option('--fix-consumers', is_flag=True, help="cleanup max-rate tree at consumers level")
def run_max_rate_tree_cleaner(zookeeper, prefix, save, fix_subscriptions, fix_consumers):

    """Removes unwanted nodes from consumers max-rate tree"""

    click.echo("""
Starting Hermes max-rate tree cleaner
=====================================
    """)

    zk = connect_to_zookeeper(zookeeper)

    if not fix_subscriptions and not fix_consumers:
        click.echo("Nothing to do")
    else:
        ensure_valid_prefix(zk, prefix)

    if save:
        confirm_calc(zookeeper, prefix)

    if fix_subscriptions:
        subscriptions = get_all_active_subscriptions(zk, prefix)

        max_rate_subscriptions = get_all_maxrate_subscriptions(zk, prefix)

        cleanup_maxrate_subscriptions(subscriptions, max_rate_subscriptions, zk, prefix, save)
        check_existing_subscriptions_maxrate(subscriptions, max_rate_subscriptions)

    if fix_consumers:
        consumers = get_consumers(zk, prefix)
        max_rate_subscriptions = get_all_maxrate_subscriptions(zk, prefix)
        cleanup_maxrate_consumers(consumers, max_rate_subscriptions, zk, prefix, save)


def ensure_valid_prefix(zk, prefix):
    valid = zk.exists(prefix)
    if not valid:
        click.echo("Invalid path: " + prefix)
        exit(1)


def confirm_calc(zookeeper, prefix):
    a = randint(0, 9)
    b = randint(0, 9)
    click.echo("Will save changes made by this script to zookeeper:")
    click.echo(" {} at path {}.".format(zookeeper, prefix))
    click.echo("Are you sure? \n\nConfirm to proceed:")

    success = False
    try:
        res = int(input("{} + {} = ? ".format(a, b)))
        success = res == a + b
    except ValueError:
        pass
    if not success:
        click.echo("Nope.")
        exit(1)
    click.echo("OK")


def get_all_active_subscriptions(zk, prefix):
    click.echo("\nExisting subscriptions:")
    active_count = 0
    not_active_count = 0
    could_not_parse_count = 0
    subscriptions = list()
    for group in zk.get_children("{}/groups".format(prefix)):
        for topic in zk.get_children("{}/groups/{}/topics".format(prefix, group)):
            for subscription in zk.get_children("{}/groups/{}/topics/{}/subscriptions".format(prefix, group, topic)):
                subscription_as_node_name = "{}.{}${}".format(group, topic, subscription)
                data, stat = zk.get("{}/groups/{}/topics/{}/subscriptions/{}".format(prefix, group, topic, subscription))
                try:
                    subscription_data = json.loads(data.decode("utf-8"))
                    state = subscription_data['state']
                    if state == 'ACTIVE':
                        subscriptions.append(subscription_as_node_name)
                        active_count += 1
                        click.echo("{}. {}".format(active_count, subscription_as_node_name))
                    else:
                        not_active_count += 1
                        click.echo("   {} is {}".format(subscription_as_node_name, state))
                except ValueError:
                    could_not_parse_count += 1
                    click.echo("Unable to read sub data: {}.{} {}".format(group, topic, subscription))

    click.echo("Found {} active subscriptions".format(active_count))
    if not_active_count > 0:
        click.echo("Found {} not active subscriptions - will clean up if needed".format(not_active_count))
    if could_not_parse_count > 0:
        click.echo("Found {} invalid subscriptions - will clean up if needed".format(could_not_parse_count))

    return subscriptions


def get_all_maxrate_subscriptions(zk, prefix):
    click.echo("\nExisting max-rate subscription nodes:")
    count = 0
    subscriptions = list()
    for subscription in zk.get_children("{}/consumers-rate/runtime".format(prefix)):
        subscriptions.append(subscription)
        count += 1
        click.echo("{}. {}".format(count, subscription))
    return subscriptions


def print_subscriptions(title, subscriptions):
    click.echo("\n{} ({}):".format(title, len(subscriptions)))
    for subscription in subscriptions:
        click.echo(" " + subscription)


def cleanup_maxrate_subscriptions(subscriptions, max_rate_subscriptions, zk, prefix, save):
    click.echo("\nCleaning max-rate subscription nodes:")
    count = 0
    for max_rate_node in max_rate_subscriptions:
        if max_rate_node not in subscriptions:
            count += 1
            path = "{}/consumers-rate/runtime/{}".format(prefix, max_rate_node)
            if save:
                click.echo(" Removing " + path)
                zk.delete(path=path, recursive=True)
                sleep(1)
            else:
                click.echo(" Would remove " + path)
    if count == 0:
        click.echo("All OK")
    else:
        click.echo("{} {} subscription nodes from max-rate tree".format("Removed" if save else "Would remove", count))


def check_existing_subscriptions_maxrate(subscriptions, max_rate_subscriptions):
    count = 0
    click.echo("\nChecking missing max-rate runtime configurations for existing subscriptions:")
    for subscription in subscriptions:
        if subscription not in max_rate_subscriptions:
            count += 1
            click.echo(" Subscription {} not found in max-rate tree".format(subscription))
    if count == 0:
        click.echo(" All OK")
    else:
        click.echo("Subscriptions missing max-rate runtime configurations: {}".format(count))


def get_consumers(zk, prefix):
    consumers = list()
    for cluster in zk.get_children("{}/consumers-workload".format(prefix)):
        for consumer in zk.get_children("{}/consumers-workload/{}/registry/nodes".format(prefix, cluster)):
            consumers.append(consumer)
    click.echo("\nActive consumers ({}):".format(len(consumers)))
    for consumer in consumers:
        click.echo(" " + consumer)
    return consumers


def cleanup_maxrate_consumers(consumers, max_rate_subscriptions, zk, prefix, save):
    click.echo("\nCleaning max-rate consumer nodes:")
    count = 0
    removed = 0
    for subscription in max_rate_subscriptions:
        count += 1
        click.echo("{}. Checking {}".format(count, subscription))
        for node in zk.get_children("{}/consumers-rate/runtime/{}".format(prefix, subscription)):
            if node not in consumers:
                removed += 1
                path = "{}/consumers-rate/runtime/{}/{}".format(prefix, subscription, node)
                if save:
                    click.echo(" Removing " + path)
                    zk.delete(path=path, recursive=True)
                    sleep(1)
                else:
                    click.echo(" Would remove " + path)
    if removed == 0:
        click.echo("All OK")
    else:
        click.echo("{} {} consumer nodes from max-rate tree".format("Removed" if save else "Would remove", removed))


if __name__ == '__main__':
    run_max_rate_tree_cleaner()
