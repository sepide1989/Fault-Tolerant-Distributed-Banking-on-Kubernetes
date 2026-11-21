"""Real CPU-intensive workload to trigger autoscaling on bank-a Pods.

Run after: kubectl -n banking port-forward svc/bank-a 18001:8000
  python scripts/load_cpu.py --url http://127.0.0.1:18001 --seconds 120 --workers 20
Verify: kubectl -n banking get hpa bank-a -w
"""
import argparse
import concurrent.futures
import time
import requests


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--url',default='http://127.0.0.1:18001')
    p.add_argument('--seconds',type=int,default=120)
    p.add_argument('--workers',type=int,default=20)
    p.add_argument('--iterations',type=int,default=50000)
    a=p.parse_args()
    deadline=time.monotonic()+a.seconds
    def hit(i):
        good=fail=0
        while time.monotonic()<deadline:
            try:
                r=requests.get(a.url+'/internal/cpu-load',params={'iterations':a.iterations},timeout=30)
                good+=int(r.status_code==200)
                fail+=int(r.status_code!=200)
            except requests.RequestException: fail+=1
        return good,fail
    started=time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
        results=list(pool.map(hit,range(a.workers)))
    print({'seconds':round(time.monotonic()-started,1),'ok':sum(x[0] for x in results),
           'failed':sum(x[1] for x in results), 'note':'HPA replica count is evidence; this only generates CPU load.'})

if __name__=='__main__':main()
