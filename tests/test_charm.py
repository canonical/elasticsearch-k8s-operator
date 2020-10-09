# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest
import yaml

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


def elastic_config(pod_spec):
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
    elsconfig = next(filter(lambda obj: obj.get('path') == 'elasticsearch.yml',
                            elsfiles), None)
    # load configuation yaml
    config_dict = yaml.safe_load(elsconfig['content'])
    return config_dict
