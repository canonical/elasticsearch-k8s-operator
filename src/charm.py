#!/usr/bin/env python3
# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import hashlib
import logging
import requests
import yaml

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import ActiveStatus, MaintenanceStatus, BlockedStatus

logger = logging.getLogger(__name__)

PEER = 'elasticsearch'
NODE_NAME = "{}-{}.{}-endpoints.{}.svc.cluster.local"
CLUSTER_SETTINGS_URL = "http://{}:{}/_cluster/settings"
SEED_SIZE = 3


class ElasticsearchOperatorCharm(CharmBase):
    _stored = StoredState()

    def __init__(self, *args):
        """Create an Elasticsearch charm

        This Elasticsearch charm supports high availability by peering
        between multiple units. It is recomended to create at least 3
        units using `juju add-unit` or using `--num-units` option of
        `juju deploy`.
        """
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on[PEER].relation_joined,
                               self._on_elasticsearch_unit_joined)
        self.framework.observe(self.on[PEER].relation_changed,
                               self._on_elasticsearch_relation_changed)
        self._stored.set_default(nodes=[self._host_name(i)
                                        for i in range(SEED_SIZE)])
        self._stored.set_default(app_ip='')

    @property
    def num_hosts(self) -> int:
        """The total number of Elasticsearch hosts.
        """
        rel = self.model.get_relation(PEER)
        return len(rel.units) + 1 if rel is not None else 1

    def _on_config_changed(self, _):
        """Set a new Juju pod specification
        """
        self._configure_pod()
        self._configure_dynamic_settings()

    def _on_stop(self, _):
        """Mark this unit as inactive
        """
        self.unit.status = MaintenanceStatus('Pod is terminating.')

    def _on_elasticsearch_unit_joined(self, _):
        """Add a new Elasticsearch node into the cluster

        Each new node uses a list seed hosts to discover other nodes
        and join the cluster. The number of seed hosts (SEED_SIZE) is
        fixed by the charm internally, and the same list of host names is
        provided to each new joining node through its pod specification.
        """
        if self.unit.is_leader():
            node_num = len(self._stored.nodes)
            # only updated the list of seed nodes if there fewer than
            # the minimum specified by this charm
            if node_num < SEED_SIZE:
                for i in range(SEED_SIZE - node_num):
                    self._stored.nodes.append(self._host_name(i))

    def _on_elasticsearch_relation_changed(self, event):
        """Reset Elasticsearch pod specification if changed
        """
        if self.unit.is_leader():
            logger.debug("Peer Node Names : {}".format(
                list(self._stored.nodes)))
        # The list of seed nodes changes only if there were fewer than
        # the minimum required. Hence a pod reconfiguration is only
        # necessary in such a case.
        if len(self._stored.nodes) < SEED_SIZE:
            self._configure_pod()

        # get the ingress-address of the elasticsearch service
        self._stored.app_ip = event.relation.data[event.unit]['ingress-address']
        self._configure_dynamic_settings()

    def _min_master_nodes(self):
        """Returns the minimum master nodes setting based on total number of nodes
        """
        return 1 if self.num_hosts <= 2 else self.num_hosts // 2 + 1

    def _build_dynamic_settings_payload(self):
        """Construct payload defining the dynamic cluster configuration settings
        """
        dynamic_config = {
            'persistent': {
                'discovery.zen.minimum_master_nodes': self._min_master_nodes(),
            },
        }

        return dynamic_config

    def _configure_dynamic_settings(self):
        """Use ES API to create dynamic config changes without pod resets
        """
        # if we don't have an app_ip (no peer units), we can safely return
        # also do not configure settings if we aren't the application leader
        if not self._stored.app_ip or not self.unit.is_leader():
            return

        url = CLUSTER_SETTINGS_URL.format(
            self._stored.app_ip,
            self.model.config['http-port'],
        )
        cluster_settings = self._build_dynamic_settings_payload()
        resp = requests.put(url, json=cluster_settings)
        if not resp.ok:
            logger.error('Could not set dynamic ES settings via the REST API. '
                         'Response code: {}'.format(resp.status_code))
            self.unit.status = BlockedStatus('Failure updating cluster-wide settings')

    def _elasticsearch_config(self):
        """Construct Elasticsearch configuration
        """
        charm_config = self.model.config

        with open('config/elasticsearch.yml') as yaml_file:
            elastic_config = yaml.safe_load(yaml_file)

        elastic_config['cluster']['name'] = charm_config['cluster-name']

        return yaml.dump(elastic_config)

    def _jvm_config(self):
        """Construct Java Virtual Machine configuration for Elasticsearch
        """
        with open('config/jvm.options') as text_file:
            return text_file.read()

    def _logging_config(self):
        """Construct the logging configuration for Elasticsearch
        """
        with open('config/logging.yml') as yaml_file:
            logging_config = yaml.safe_load(yaml_file)

        return yaml.dump(logging_config)

    def _log4j_config(self):
        """Construct the Log4J configuration for Elasticsearch
        """
        with open('config/log4j2.properties') as text_file:
            return text_file.read()

    def _host_name(self, node_num):
        """Hostname of the nth Juju unit for this charm
        """
        return NODE_NAME.format(self.meta.name,
                                node_num,
                                self.meta.name,
                                self.model.name)

    def _seed_hosts(self):
        """Generate the list of seed host names

        This list is used to populate the unicast_hosts.txt file used by
        Elasticsearch.
        """
        seed_hosts = list(self._stored.nodes)
        logger.debug('Seed Hosts : {}'.format(seed_hosts))

        return '\n'.join(seed_hosts)

    def _config_hash(self):
        """Fingerprint for an Elasticsearch configuration setup

        This has the complete set of Elasticsearch configuration files
        is used to set an environment variable in the application
        container. This is necessary so that any updated to the
        configuration (essentially a ConfigMap in Kubernetes) does
        indeed make Juju trigger the creation of pods using the
        updated configuration.
        """
        config_string = self._seed_hosts() + self._elasticsearch_config() +\
            self._jvm_config() + self._logging_config() + self._log4j_config()

        return hashlib.md5(config_string.encode()).hexdigest()

    def _build_pod_spec(self):
        """Construct a Juju pod specification for Elasticsearch
        """
        logger.debug('Building Pod Spec')
        charm_config = self.model.config
        spec = {
            'version': 3,
            'containers': [{
                'name': self.app.name,
                'imageDetails': {
                    'imagePath': charm_config['elasticsearch-image-path'],
                },
                'ports': [{
                    'containerPort': charm_config['http-port'],
                    'protocol': 'TCP'
                }],
                'envConfig': {
                    'ES_PATH_CONF': '/etc/elasticsearch',
                    'ES_CONFIG_HASH': self._config_hash()
                },
                'volumeConfig': [{
                    'name': 'config',
                    'mountPath': '/usr/share/elasticsearch/config',
                    'files': [{
                        'path': 'unicast_hosts.txt',
                        'content': self._seed_hosts()
                    }, {
                        'path': 'elasticsearch.yml',
                        'content': self._elasticsearch_config()
                    }, {
                        'path': 'jvm.options',
                        'content': self._jvm_config()
                    }, {
                        'path': 'logging.yml',
                        'content': self._logging_config()
                    }, {
                        'path': 'log4j2.properties',
                        'content': self._log4j_config()
                    }]
                }],
                'kubernetes': {
                    'livenessProbe': {
                        'httpGet': {
                            'path': '/_cat/health?v',
                            'port': charm_config['http-port']
                        },
                        'initialDelaySeconds': 30,
                        'timeoutSeconds': 30
                    },
                },
            }]
        }

        return spec

    def _configure_pod(self):
        """Setup a new Elasticsearch Pod specification
        """
        if not self.unit.is_leader():
            self.unit.status = ActiveStatus()
            return

        logger.debug('Configuring dynamic settings so pod '
                     'does not have to restart')

        logger.debug('Configuring Pod')
        self.unit.status = MaintenanceStatus('Setting pod spec')
        pod_spec = self._build_pod_spec()

        self.model.pod.set_spec(pod_spec)
        self.app.status = ActiveStatus('Elasticsearch is ready')
        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(ElasticsearchOperatorCharm)
