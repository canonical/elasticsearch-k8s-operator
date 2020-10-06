# elasticsearch-operator

## Description

The Elasticsearch Operator provides a distributed search and analysis solution
using [Elasticsearch](https://www.elastic.co/).

## Setup
Increase the maximum number of virtual memory areas on your host
system. On a Linux system this can be done using the command line

    sudo sysctl -w vm.max_map_count=262144

For a more permanent change edit `/etc/sysctl.conf`.

## Build

To build the charm, first install the `charmcraft` tool

    sudo snap install charmcraft --classic

Then in this git repository run the command

    charmcraft build

## Usage

TODO: explain how to use the charm

## Developing

Use your Python 3 development environment or create and activate a virtualenv,
and install the development requirements,

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt

## Testing

Just run `run_tests`:

    ./run_tests
