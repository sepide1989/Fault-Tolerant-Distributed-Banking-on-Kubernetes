"""Import an already built Docker image through temporary node maintenance pods."""
import argparse
from pathlib import Path
import subprocess


def main():
    p=argparse.ArgumentParser();p.add_argument('--image',required=True)
    p.add_argument('--pods',nargs='+',required=True);a=p.parse_args()
    path=Path('reports/assignment3/private/recovery-image.tar')
    path.parent.mkdir(parents=True,exist_ok=True)
    subprocess.run(['docker','save',a.image,'-o',str(path)],check=True,timeout=180)
    for pod in a.pods:
        with path.open('rb') as stream:
            subprocess.run(['kubectl','-n','banking','exec','-i',pod,'--','chroot','/host',
                'ctr','-n','k8s.io','images','import','-'],stdin=stream,check=True,timeout=300)
        print('Imported',a.image,'through',pod,flush=True)


if __name__=='__main__':main()
