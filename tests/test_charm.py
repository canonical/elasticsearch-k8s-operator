# Copyright 2020 Balbir Thomas
# See LICENSE file for licensing details.

import unittest
import yaml

from ops.testing import Harness
from charm import ElasticsearchOperatorCharm

MINIMAL_CONFIG = {
    'elasticsearch-image-path': 'elastic',
    'cluster-name': 'elasticsearch',
    'advertised-port': 9200
}


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(ElasticsearchOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    @unittest.skip("Configuation file : not yet implemented")
    def test_cluster_name_can_be_changed(self):
        self.harness.set_leader(True)
        name_config = MINIMAL_CONFIG.copy()
        name_config['cluster-name'] = 'new name'
        self.harness.update_config(name_config)
        pod_spec, _ = self.harness.get_pod_spec()
        config = elastic_config(pod_spec)
        self.assertEqual(config['cluster']['name'],
                         name_config['cluster-name'])


def elastic_config(pod_spec):
    config_yaml = pod_spec[0]['containers'][0]['files'][0]['files']['elasticsearch.yml']
    config_dict = yaml.safe_load(config_yaml)
    return config_dict
