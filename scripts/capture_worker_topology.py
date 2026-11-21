"""Capture UID-bound PG/PVC/PV placement before a single-worker loss drill."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--output');a=p.parse_args()
    result={'created_utc':cs.utc(),'clusters':{n:cs.database_cluster(n) for n in cs.CLUSTERS},
            'nodes':cs.sanitize(cs.get('nodes')),'pvcs':cs.sanitize(cs.get('pvc')),
            'pvs':cs.sanitize(cs.get('pv'))}
    if a.output:cs.write_report(a.output,result)
    else:print(json.dumps(result,indent=2))


if __name__=='__main__':main()
