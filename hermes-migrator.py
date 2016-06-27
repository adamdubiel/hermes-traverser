import click
import json
import requests

@click.command()
@click.option('--source', '-s', required = True, help = 'URL to source Hermes Management')
@click.option('--destination', '-d', required = True, help = 'URL to destination Hermes Management')
@click.option('--authheader', '-a', required = True, help = 'Authorization header to send')
@click.option('--authkey', '-k', required = True, help = 'Authorization header value')
@click.option('--dryrun', is_flag = True, help = "dry run mode won't modify anything")
def migrator(source, destination, authheader, authkey, dryrun):

    """Migrates structure between Hermes clusters"""

    click.echo("""
Starting Hermes Migrator
==================================
    """)

    auth = {authheader: authkey}

    groups = fetchGroups(source)
    migrate(groups, source, destination, auth, dryrun)


def fetchGroups(source):
    return requests.get("{}/groups".format(source)).json()

def migrate(groups, source, destination, auth, dryrun):
    allTopics = requests.get("{}/topics".format(source)).json()
    for group in groups:
        migrateGroup(group, topicsForGroup(allTopics, group), source, destination, auth, dryrun)

def topicsForGroup(allTopics, group):
    return [t for t in allTopics if t.rsplit('.', 1)[0] == group]

def migrateGroup(group, topics, source, destination, auth, dryrun):
    sourceGroupRequest = requests.get(groupUrl(source, group))
    if sourceGroupRequest.status_code != 200:
        return
    sourceGroupBody = sanitizeGroup(sourceGroupRequest.json())

    r = requests.get(groupUrl(destination, group))
    if r.status_code == 500 or r.status_code == 404:
        if r.status_code == 500:
            run(lambda: requests.delete(groupUrl(destination, group), headers = auth), dryrun, "Deleting corrupted group: {}".format(group))
        run(lambda: requests.post("{}/groups".format(destination), headers = auth, json = sourceGroupBody), dryrun, "Creating missing group: {}".format(group))
    else:
        run(lambda: requests.put(groupUrl(destination, group), headers = auth, json = sourceGroupBody), dryrun, "Patching existing group: {}".format(group))
    migrateTopics(topics, source, destination, auth, dryrun)

def sanitizeGroup(group):
    if 'contact' not in group:
        group['contact'] = 'undefined'
    if 'supportTeam' not in group:
        group['supportTeam'] = 'undefined'
    return group

def groupUrl(host, group):
    return "{}/groups/{}".format(host, group)

def migrateTopics(topics, source, destination, auth, dryrun):
    for topic in topics:
        migrateTopic(topic, source, destination, auth, dryrun)

def migrateTopic(topic, source, destination, auth, dryrun):
    sourceTopicRequest = requests.get(topicUrl(source, topic))
    if sourceTopicRequest.status_code != 200:
        return

    sourceTopicBody = sanitizeTopic(sourceTopicRequest.json())
    schemaRequest = requests.get("{}/topics/{}/schema".format(source, topic))
    sourceHasSchema = schemaRequest.status_code == 200
    if sourceHasSchema:
        schema = schemaRequest.json()

    r = requests.get(topicUrl(destination, topic))
    if r.status_code == 500 or r.status_code == 404:
        if r.status_code == 500:
            run(lambda: requests.delete(topicUrl(destination, topic), headers = auth), dryrun, "Deleting corrupted topic: {}".format(topic))
        run(lambda: requests.post("{}/topics".format(destination), headers = auth, json = sourceTopicBody), dryrun, "Creating missing topic: {}".format(topic))
        if sourceHasSchema:
            if requests.get("{}/topics/{}/schema".format(destination, topic)).status_code == 204:
                run(lambda: requests.post("{}/topics/{}/schema".format(destination, topic), headers = auth, json = schema), dryrun, "Creating missing schema for topic: {}".format(topic))

def sanitizeTopic(topicBody):
    if 'migratedFromJsonType' in topicBody:
        topicBody['migratedFromJsonType'] = False
    if 'description' not in topicBody:
        topicBody['description'] = 'no description'
    if topicBody['retentionTime']['duration'] < 1:
        topicBody['retentionTime']['duration'] = 1
    return topicBody

def topicUrl(host, topic):
    return "{}/topics/{}".format(host, topic)


def run(f, dryrun, text):
    if dryrun:
        message = "DRYRUN: {}".format(text)
    else:
        message = text
    click.echo(message)

    if not dryrun:
        r = f()
        if r.status_code == 400 or r.status_code == 500 or r.status_code == 403:
            click.echo("{}".format(r.json()))
        r.raise_for_status()

if __name__ == '__main__':
    migrator()
