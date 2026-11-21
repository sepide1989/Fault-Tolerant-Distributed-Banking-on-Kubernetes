"""Review/apply worker placement without replacing live DB/plugin configuration."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--apply',action='store_true');a=p.parse_args()
    for patch in yaml.safe_load_all(Path('k8s/bonus/02-worker-app-placement.yaml').read_text()):
        name=patch['metadata']['name'];current=cs.get('deployment',name)
        print(name,json.dumps(patch['spec']))
        if a.apply:
            cs.kb('patch','deployment',name,'--type=strategic','-p',json.dumps({'spec':patch['spec']}))
    if not a.apply:print('DRY RUN; no resources changed')


if __name__=='__main__':main()
