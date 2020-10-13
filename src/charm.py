#!/usr/bin/env python3
# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import yaml
import hashlib

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)

PEER = 'elasticsearch'
NODE_NAME = "{}-{}.{}-endpoints.{}.svc.cluster.local"
SEED_SIZE = 3


class ElasticsearchOperatorCharm(CharmBase):
    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on[PEER].relation_joined,
                               self._on_elasticsearch_unit_joined)
        self.framework.observe(self.on[PEER].relation_changed,
                               self._on_elasticsearch_relation_changed)
        self._stored.set_default(nodes=[NODE_NAME.format(self.meta.name,
                                                         i,
                                                         self.meta.name,
                                                         self.model.name)
                                        for i in range(SEED_SIZE)])

    def _on_config_changed(self, _):
        """Set a new Juju pod specification
        """
        self._configure_pod()

    def _on_stop(self, _):
        """Mark unit is inactive
        """
        self.unit.status = MaintenanceStatus('Pod is terminating.')

    def _on_elasticsearch_unit_joined(self, event):
        if self.unit.is_leader():
            node_num = len(self._stored.nodes)
            if node_num < SEED_SIZE:
                self._stored.nodes.append(self._host_name(node_num))

    def _on_elasticsearch_relation_changed(self, event):
        if self.unit.is_leader():
            logger.debug("Peer Node Names : {}".format(
                list(self._stored.nodes)))
        if len(self._stored.nodes) < SEED_SIZE:
            self._configure_pod()

    def _elasticsearch_config(self):
        """Construct Elasticsearch configuration
        """
        charm_config = self.model.config

        with open('config/elasticsearch.yml') as yaml_file:
            elastic_config = yaml.safe_load(yaml_file)

        elastic_config['cluster']['name'] = charm_config['cluster-name']

        return yaml.dump(elastic_config)

    def _jvm_config(self):
        with open('config/jvm.options') as text_file:
            return text_file.read()

    def _logging_config(self):
        with open('config/logging.yml') as yaml_file:
            logging_config = yaml.safe_load(yaml_file)

        return yaml.dump(logging_config)

    def _log4j_config(self):
        with open('config/log4j2.properties') as text_file:
            return text_file.read()

    def _host_name(self, node_num):
        return NODE_NAME.format(self.meta.name,
                                node_num,
                                self.meta.name,
                                self.model.name)

    def _seed_hosts(self):
        seed_hosts = list(self._stored.nodes)
        logger.debug('Seed Hosts : {}'.format(seed_hosts))

        return '\n'.join(seed_hosts)

    def _config_hash(self):
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

        logger.debug('Configuring Pod')

        self.unit.status = MaintenanceStatus('Setting pod spec')
        pod_spec = self._build_pod_spec()

        self.model.pod.set_spec(pod_spec)
        self.app.status = ActiveStatus('Elasticsearch is ready')
        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(ElasticsearchOperatorCharm)
