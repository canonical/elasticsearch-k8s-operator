#!/usr/bin/env python3
# Copyright 2020 Balbir Thomas
# See LICENSE file for licensing details.

import logging
import yaml

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)

PEER = 'elasticsearch'


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
        self._stored.set_default(nodes=set())

    def _on_config_changed(self, _):
        """Set a new Juju pod specification
        """
        self._configure_pod()

    def _on_stop(self, _):
        """Mark unit is inactive
        """
        self.unit.status = MaintenanceStatus('Pod is terminating.')

    def _on_elasticsearch_unit_joined(self, event):
        address = self.model.get_binding(event.relation).network.bind_address
        event.relation.data[self.unit]['bind-address'] = str(address)

    def _on_elasticsearch_relation_changed(self, event):
        if self.unit.is_leader():
            logger.debug("Old Peer Node Addresses : {}".format(
                list(self._stored.nodes)))
            address = event.relation.data[event.unit].get('bind-address')
            if address:
                logger.debug("New Peer Node Address : {}".format(address))
                self._stored.nodes.add(str(address))

    def _elasticsearch_config(self):
        """Construct Elasticsearch configuation
        """
        charm_config = self.model.config

        with open('config/elasticsearch.yml') as yaml_file:
            elastic_config = yaml.safe_load(yaml_file)

        elastic_config['cluster']['name'] = charm_config['cluster-name']

        return yaml.dump(elastic_config)

    def _build_pod_spec(self):
        """Construct a Juju pod specification for Elasticsearch
        """
        logger.debug('Building Pod Spec')
        charm_config = self.model.config
        spec = {
            'containers': [{
                'name': self.app.name,
                'imageDetails': {
                    'imagePath': charm_config['elasticsearch-image-path'],
                },
                'livenessProbe': {
                    'httpGet': {
                        'path': '/_cat/health?v',
                        'port': charm_config['advertised-port']
                    },
                    'initialDelaySeconds': 30,
                    'timeoutSeconds': 30
                },
                'ports': [{
                    'containerPort': charm_config['advertised-port'],
                    'name': 'api-port',
                    'protocol': 'TCP'
                }]
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
