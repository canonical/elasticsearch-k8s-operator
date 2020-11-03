#!/usr/bin/env python3
# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import hashlib
import logging
import traceback
import yaml

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import ActiveStatus, MaintenanceStatus, BlockedStatus

logger = logging.getLogger(__name__)

PEER = 'elasticsearch'
CLUSTER_SETTINGS_URL = "http://{}/_cluster/settings"
NODE_NAME = "{}-{}.{}-endpoints.{}.svc.cluster.local"
SEED_SIZE = 3


class ElasticsearchOperatorCharm(CharmBase):
    _stored = StoredState()

    def __init__(self, *args):
        """Create an Elasticsearch charm

        This Elasticsearch charm supports high availability by peering
        between multiple units. It is recommended to create at least 3
        units using `juju add-unit` or using `--num-units` option of
        `juju deploy`.
        """
        super().__init__(*args)

        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(
            self.on[PEER].relation_joined,
            self._on_elasticsearch_unit_joined
        )
        self.framework.observe(
            self.on[PEER].relation_changed,
            self._on_elasticsearch_relation_changed
        )
        self.framework.observe(
            self.on['datastore'].relation_changed,
            self._on_datastore_relation_changed
        )

        self._stored.set_default(nodes=[self._host_name(i)
                                        for i in range(SEED_SIZE)])

    @property
    def num_hosts(self) -> int:
        """The total number of Elasticsearch hosts
        """
        rel = self.model.get_relation(PEER)
        return len(rel.units) + 1 if rel is not None else 1

    @property
    def num_es_nodes(self) -> int:
        """The number of nodes recognized by the Elasticsearch cluster

        In the case of a RequestError, return 0
        """
        es = self._get_es_client()
        try:
            health = es.cat.health(format='json', h='node.total')
            return int(health[0]['node.total'])
        except RequestError:
            return 0

    @property
    def current_minimum_master_nodes(self) -> int:
        """The current value of the discovery.zen.minimum_master_nodes cluster setting

        Default to 1 if we cannot get the value.
        """
        # attempt to get the settings of the cluster via the python module
        es = self._get_es_client()
        try:
            settings = es.cluster.get_settings()
        except RequestError:
            return 1

        # attempt to get the minimum_master_node setting from the nested dictionary
        try:
            return int(settings['persistent']['discovery']['zen']['minimum_master_nodes'])
        except KeyError:
            logger.warning('minimum_master_nodes not found in cluster settings')
            return 1

    @property
    def ideal_minimum_master_nodes(self):
        """Returns the minimum master nodes setting based on total number of nodes
        """
        return 1 if self.num_hosts <= 2 else self.num_hosts // 2 + 1

    @property
    def ingress_address(self) -> str:
        """The ingress-address of the Elasticsearch cluster
        """
        return str(self.model.get_binding(PEER).network.ingress_address)

    def _on_config_changed(self, _):
        """Set a new Juju pod specification
        """
        self._configure_pod()

    def _on_update_status(self, _):
        """Update status event to take care of various cluster health checks
        """
        # check to see if we need to update the dynamic settings
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

    def _on_elasticsearch_relation_changed(self, _):
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

        # attempt to configure dynamic settings of the cluster
        self._configure_dynamic_settings()

    def _on_datastore_relation_changed(self, event):
        """This event handler only needs to pass the port to the remote unit

        The related app will automatically have access to the ingress-address
        of the Elasticsearch cluster.
        """
        if self.unit.is_leader():
            event.relation.data[self.unit]['port'] = str(self.model.config['http-port'])

    def _build_dynamic_settings_payload(self):
        """Construct payload of the cluster configuration settings that need updating
        """
        dynamic_config = {
            'persistent': {},
            'transient': {},
        }

        # determine whether minimum_master_nodes setting needs to be updated
        if self.ideal_minimum_master_nodes != self.current_minimum_master_nodes:
            dynamic_config['persistent'].update({
                'discovery.zen.minimum_master_nodes': self.ideal_minimum_master_nodes
            })

        # check whether there have been any new settings that need changing
        if not dynamic_config['persistent'] and not dynamic_config['transient']:
            return None
        else:
            return dynamic_config

    def _get_es_client(self) -> Elasticsearch():
        """Return an instance of the Elasticsearch Python client

        ES Python module docs:
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch
        """
        # if we don't have an ingress_address (no peer units), it means we are unable
        # to access the application ingress-address and cannot create an ES Python client
        host = '{}:{}'.format(
            self.ingress_address,
            self.model.config['http-port']
        )

        # TODO: if credentials are added to the config options, be sure to
        #       add them in the instantiation of the ES client
        return Elasticsearch(host)

    def _configure_dynamic_settings(self):
        """Use ES API to create dynamic config changes without pod resets

        A dynamic setting update cannot (and will not) take place if the number of units
        recognized by Juju does not match the number of nodes recognized by Elasticsearch
        """
        if self.num_hosts != self.num_es_nodes:
            self.unit.status = MaintenanceStatus('Waiting for nodes to join ES cluster')
            return
        elif not self.unit.is_leader():
            self.unit.status = ActiveStatus()
            return

        cluster_settings = self._build_dynamic_settings_payload()
        if cluster_settings is None:
            self.unit.status = ActiveStatus()
            return

        # attempt to make cluster settings changes
        es = self._get_es_client()
        try:
            logger.info('Attempting to configure dynamic settings.')
            es.cluster.put_settings(body=cluster_settings)
            self.unit.status = ActiveStatus()
        except RequestError:
            logger.error(traceback.format_exc())
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
                        'initialDelaySeconds': 20,
                        'timeoutSeconds': 20,
                    },
                    'readinessProbe': {
                        'httpGet': {
                            'path': '/_cat/health?v',
                            'port': charm_config['http-port']
                        },
                        'initialDelaySeconds': 10,
                        'timeoutSeconds': 10,
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
