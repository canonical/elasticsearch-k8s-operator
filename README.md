# elasticsearch-operator

## Description

The Elasticsearch Operator provides a distributed search and analysis solution
using [Elasticsearch](https://www.elastic.co/).

## Setup
Increase the maximum number of virtual memory areas on your host
system. On a Linux system this can be done using the command line

    sudo sysctl -w vm.max_map_count=262144

For a more permanent change edit `/etc/sysctl.conf`.

## Install Dependencies and Build

To build the charm, first install `charmcraft`,  `juju` and `microk8s`

    snap install charmcraft
    snap install juju --classic
    snap install microk8s --classic 

Then in this git repository run the command

    charmcraft build

## Usage

    juju deploy ./elasticsearch.charm

To scale up:

    juju add-unit -n 2 elasticsearch

> Note: When the total number of nodes in the cluster is 2, split brain is possible. If there are currently two nodes, be sure to use `juju add-unit` to scale up to a functional HA cluster. For more information about how Elasticsearch handles quorum and the effects of split brain, [take a look at the Elasticsearch docs](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/modules-discovery-quorums.html).

To check the status of the cluster:

    # 1. wait until all units and application is active
    watch -c juju status --color
    # 2. copy the application IP address from the juju status output
    # 3. check the health of the cluster
    curl -X GET http://{APP_IP}:9200/_cat/health?v&pretty
    

## Developing

Use your Python 3 development environment or create and activate a virtualenv,
and install the development requirements,

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt

## Testing

Just run `run_tests`:

    ./run_tests
