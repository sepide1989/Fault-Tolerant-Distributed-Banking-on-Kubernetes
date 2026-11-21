"""Static contract tests, no cluster or PostgreSQL required.

Install for local-only checks: pip install pyyaml
Run: python -m unittest discover -s tests/assignment3 -p 'test_static_contract.py'
Real failure experiments require Kubernetes; these tests are not substitutes.
"""
from pathlib import Path
import unittest

import yaml

ROOT=Path(__file__).resolve().parents[2]


def docs(name):
    return [d for d in yaml.safe_load_all((ROOT/name).read_text()) if d]


class KubernetesContracts(unittest.TestCase):
    def test_base_clusters_sync(self):
        cls=docs('k8s/base/01-clusters.yaml')
        self.assertEqual({c['metadata']['name'] for c in cls}, {'bank-a','bank-b','bank-meta'})
        for c in cls:
            self.assertEqual(c['spec']['instances'],2)
            self.assertEqual(c['spec']['postgresql']['synchronous']['dataDurability'],'required')
            self.assertEqual(c['spec']['postgresql']['synchronous']['number'],1)
            self.assertGreater(int(c['spec']['postgresql']['parameters']['max_prepared_transactions']),0)

    def test_bonus_clusters_durable_and_independent_local_pvc(self):
        cls=docs('k8s/bonus/01-three-vm-db.yaml')
        self.assertEqual(len(cls),3)
        for c in cls:
            s=c['spec']
            self.assertEqual(s['instances'],3)
            self.assertEqual(s['storage']['storageClass'],'local-path')
            self.assertEqual(s['affinity']['nodeSelector'],{'banking-worker':'true'})
            self.assertEqual(s['postgresql']['synchronous']['number'],2)
            self.assertEqual(s['postgresql']['synchronous']['dataDurability'],'required')

    def test_app_services_and_rollouts(self):
        apps=docs('k8s/base/03-apps.yaml')
        services=docs('k8s/base/04-services.yaml')
        deploys={a['metadata']['name']:a for a in apps}
        self.assertTrue({'bank-a','bank-b','gateway','coordinator-recovery'} <= set(deploys))
        self.assertTrue({'bank-a','bank-b','gateway'} <= {a['metadata']['name'] for a in services})
        for name in ('bank-a','bank-b','gateway'):
            spec=deploys[name]['spec']
            self.assertEqual(spec['strategy']['rollingUpdate']['maxUnavailable'],0)
            c=spec['template']['spec']['containers'][0]
            self.assertIn('livenessProbe',c)
            self.assertIn('readinessProbe',c)
            self.assertIn('requests',c['resources'])
            self.assertIn('limits',c['resources'])

    def test_configmap_and_operator_compatibility(self):
        config=docs('k8s/base/00-configmap.yaml')
        self.assertEqual(len(config),1)
        self.assertEqual(config[0]['kind'],'ConfigMap')
        self.assertEqual(config[0]['data']['PREPARE_LOCK_TIMEOUT'],'5s')
        for app in docs('k8s/base/03-apps.yaml'):
            if app['metadata']['name'] in ('bank-a','bank-b'):
                container=app['spec']['template']['spec']['containers'][0]
                self.assertEqual(container['envFrom'][0]['configMapRef']['name'],'bank-runtime-config')
        for name in ('scripts/deploy_minikube.sh','scripts/deploy_minikube.ps1','scripts/install_three_vm_addons.sh'):
            self.assertIn('release-1.30/releases/cnpg-1.30.1.yaml',(ROOT/name).read_text())

    def test_hpa_policy_backup(self):
        hpas=docs('k8s/base/05-hpa-pdb.yaml')
        self.assertEqual(sum(x['kind']=='HorizontalPodAutoscaler' for x in hpas),2)
        netpol=docs('k8s/base/06-networkpolicy.yaml')
        self.assertEqual(len(netpol),2)
        for p in netpol:
            self.assertEqual(p['kind'],'NetworkPolicy')
            self.assertEqual(p['spec']['policyTypes'],['Ingress'])
        restore=docs('k8s/backup/04-restore-as-new-cluster.yaml')
        self.assertEqual({x['metadata']['name'] for x in restore},
                         {'bank-a-restore','bank-b-restore','bank-meta-restore'})
        self.assertTrue(all(x['spec']['storage']['storageClass']=='local-path' for x in restore))

    def test_sql_journal_durable(self):
        text=(ROOT/'gateway/coordinator_pg.py').read_text()
        self.assertIn('pg_try_advisory_lock',text)
        self.assertIn("decision='COMMIT'",text)
        self.assertIn('def recover_once',text)
        self.assertNotIn('sqlite3',text)


if __name__=='__main__':unittest.main()
