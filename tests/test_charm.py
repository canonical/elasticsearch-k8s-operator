# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import random
import unittest
import yaml

import charm
from ops.testing import Harness
from charm import ElasticsearchOperatorCharm

MINIMAL_CONFIG = {
    'elasticsearch-image-path': 'elastic',
    'cluster-name': 'elasticsearch',
    'http-port': 9200
}


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(ElasticsearchOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_cluster_name_can_be_changed(self):
        self.harness.set_leader(True)
        name_config = MINIMAL_CONFIG.copy()
        name_config['cluster-name'] = 'new name'
        self.harness.update_config(name_config)
        pod_spec, _ = self.harness.get_pod_spec()
        config = elastic_config(pod_spec)
        self.assertEqual(config['cluster']['name'],
                         name_config['cluster-name'])

    def test_seed_nodes_are_added_when_fewer_than_minimum(self):
        self.harness.set_leader(True)
        seed_config = MINIMAL_CONFIG.copy()
        self.harness.update_config(seed_config)

        # create a peer relation and add a peer unit
        rel_id = self.harness.add_relation('elasticsearch', 'elasticsearch')
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, 'elasticsearch-operator-0')

        # check number of seed hosts is the default value
        pod_spec, _ = self.harness.get_pod_spec()
        seed_hosts_file = config_file(pod_spec, 'unicast_hosts.txt')
        self.assertEqual(charm.SEED_SIZE, len(seed_hosts_file['content'].split("\n")))

        # increase number of seed hosts and add a unit to trigger the change
        charm.SEED_SIZE = 4
        self.harness.add_relation_unit(rel_id, 'elasticsearch-operator-1')
        self.harness.update_config(seed_config)

        # check the number of seed hosts has now increased
        pod_spec, _ = self.harness.get_pod_spec()
        seed_hosts_file = config_file(pod_spec, 'unicast_hosts.txt')
        self.assertEqual(charm.SEED_SIZE, 4)
        self.assertEqual(charm.SEED_SIZE, len(seed_hosts_file['content'].split("\n")))

    def test_num_hosts_is_equal_to_num_units(self):
        self.harness.set_leader(True)
        seed_config = MINIMAL_CONFIG.copy()
        self.harness.update_config(seed_config)

        # add a random number of peer units
        rel_id = self.harness.add_relation('elasticsearch', 'elasticsearch')
        self.assertIsInstance(rel_id, int)
        num_units = random.randint(2, 10)

        # elasticsearch-operator/0 already exists as the starting unit
        for i in range(1, num_units):
            self.harness.add_relation_unit(rel_id, 'elasticsearch-operator/{}'.format(i))
        self.assertEqual(self.harness.charm.num_hosts, num_units)

    def test_minimum_master_nodes_matches_formula(self):
        # Test whether _minimum_master_nodes function
        # matches formula N / 2 + 1 for N > 2, 1 otherwise
        self.harness.set_leader(True)
        seed_config = MINIMAL_CONFIG.copy()
        self.harness.update_config(seed_config)
        rel_id = self.harness.add_relation('elasticsearch', 'elasticsearch')

        # create inputs of three main case categories with two examples each
        # case categories: num_nodes <= 2, num_nodes is even, num_nodes is odd
        total_mmn_cases = [(1, 1), (2, 1), (4, 3), (5, 3), (6, 4), (7, 4)]
        for (num_nodes, expected_mmn) in total_mmn_cases:
            with self.subTest():
                for i in range(1, num_nodes):
                    self.harness.add_relation_unit(rel_id, 'elasticsearch-operator/{}'.format(i))
                actual_mmn = self.harness.charm._min_master_nodes()
                self.assertEqual(expected_mmn, actual_mmn)

    def test_dynamic_settings_payload_has_correct_minimum_master_nodes(self):
        self.harness.set_leader(True)
        seed_config = MINIMAL_CONFIG.copy()
        self.harness.update_config(seed_config)

        # create the peer relation
        rel_id = self.harness.add_relation('elasticsearch', 'elasticsearch')

        # when number of nodes is 6, min master nodes should be 6 / 2 + 1 = 4
        num_nodes = 6
        expected_mmn = 4
        for i in range(1, num_nodes):
            self.harness.add_relation_unit(rel_id, 'elasticsearch-operator/{}'.format(i))
        payload = self.harness.charm._build_dynamic_settings_payload()
        actual_mmn = payload['persistent']['discovery.zen.minimum_master_nodes']
        self.assertEqual(expected_mmn, actual_mmn)


def config_file(pod_spec, file):
    # get elasticsearch container from pod spec
    containers = pod_spec['containers']
    elspod = next(filter(lambda obj: obj.get('name') == 'elasticsearch-operator',
                         containers), None)
    # get mounted configuration volume from container spec
    elsvolumes = elspod['volumeConfig']
    elsconfig = next(filter(lambda obj: obj.get('name') == 'config',
                            elsvolumes), None)
    # get elasticsearch configuation file from configuation volume
    elsfiles = elsconfig['files']
    conf_file = next(filter(lambda obj: obj.get('path') == file,
                            elsfiles), None)
    return conf_file


def elastic_config(pod_spec):
    elsconfig = config_file(pod_spec, 'elasticsearch.yml')

    # load configuation yaml
    config_dict = yaml.safe_load(elsconfig['content'])
    return config_dict
