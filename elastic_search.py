#!python

"""
Usage:
    python elastic_search.py --from <YYYY-MM-DD> --to <YYYY-MM-DD> --exact urls.txt

Will read ULRs from a file (or STDIN if no file specified) - one URL per line
For each URL, will run a query on WebScraper ElasticSearch to retrieve available Page Object S3 locations
Date range can be specified with command line options
    --from <YYYY-MM-DD> or -f <YYYY-MM-DD>
    --to <YYYY-MM-DD> or -t <YYYY-MM-DD>
    'now' can be used to stand in for current time
    will default to looking for events for the past 7 days
--exact or -e will display only exact matches
--finalState <pattern> or -s <pattern> will only match "finalState=<pattern>", default: crawled
--crawlResult <pattern> or -r <pattern> will only match "crawlResult=<pattern>", default: SUCCESS
--matchByDomain or -d will treat the url list as domain list
--allResults or -a will attempt to retrieve all available results, but default will stop at 10,000
--countOnly or -c will only count the number of results, not retrieve them
"""

import requests
import sys
import getopt
from datetime import datetime
from datetime import timedelta

# Crawler History production OpenSearch URL

request_url = "https://vpc-dispatcher-opnsrch-prod-32ofqlqiqge6324cmjaqgo7v6i.us-east-1.es.amazonaws.com/_search"
SCROLL_QUERY_PARAM = "scroll=10m"

# setting up defaults
from_date = f'{(datetime.now() + timedelta(days=-7)).isoformat()}'
to_date = f'{(datetime.now()).isoformat()}'
in_file = sys.stdin
exact_match = False
finalState = 'crawled'
crawlResult = 'SUCCESS'
resultSize = 10000
matchField = 'url'

# Counters
progress_count = 0
found = 0
matched = 0

def is_filter_ok(orig_url, result_obj):
    return (not exact_match or orig_url == result_obj[matchField])

# process CLI args
opts, args = getopt.getopt(sys.argv[1:], 
    'f:t:es:r:dac', 
    ['from=', 'to=', 'exact', 'finalStage=', 'crawlResult=', 'matchByDomain', 'allResults', 'countOnly'])

for (opt, val) in opts:
    if opt in ('--from', '-f'):             from_date = val 
    if opt in ('--to', '-t'):               to_date = val 
    if opt in ('--exact', '-e'):            exact_match = True
    if opt in ('--finalState', '-s'):       finalState = val
    if opt in ('--crawlResult', '-r'):      crawlResult = val
    if opt in ('--matchByDomain', '-d'):    matchField = 'domain'
    if opt in ('--allResults', '-a'):       request_url = f'{request_url}?{SCROLL_QUERY_PARAM}'
    if opt in ('--countOnly', '-c'):        request_url = request_url.replace('_search', '_count')

if len(args) > 0: in_file = open(args[0]) 

# run
with in_file:
    print('url,page_object,fetch_time')
    for url in in_file:
        progress_count += 1
        if progress_count % 100 == 0:
            print(f"{datetime.now()} Processing {progress_count}'s url, found:{found} / matched:{matched}",
                   file=sys.stderr)

        url = url.strip()
        q = {
            "size": resultSize,
            "query": {
                "bool": {
                    "must": [
                        {"range":
                            { "dispatcherEntryTime":
                                {"format": "strict_date_optional_time", "gte": from_date, "lte": to_date}}}
                        , {"match": {"finalState": {"query": finalState}}}
                        , {"match": {"crawlResult": {"query": crawlResult}}}
                        , {"match": {matchField: f"{url}"}}
                    ]
                }
            }
        }

        if request_url.endswith('/_count'):
            q.pop('size', None)
            
        while True:
            r = requests.get(request_url, json=q)
            if r.ok:
                result = r.json()
                if request_url.endswith('/_count'):
                    print(f'{url},{result["count"]}')
                    break
                elif len(result["hits"]["hits"]) > 0:
                    found += len(result["hits"]["hits"])
                    if "_scroll_id" in result:
                        request_url = request_url.replace(f"?{SCROLL_QUERY_PARAM}", '/scroll')
                        q["scroll_id"] = f"{result['_scroll_id']}"
                        q.pop('size', None)
                        q.pop('query', None)
                    objs = sorted(result["hits"]["hits"],
                                key=lambda record: datetime.fromisoformat(record["_source"]["fetchTime"] or datetime.now().isoformat()).timestamp(),
                                reverse=True)

                    for obj in objs:
                        if is_filter_ok(url, obj["_source"]):
                            print(f'{obj["_source"]["url"]},{obj["_source"]["pageObject"]},{obj["_source"]["fetchTime"]}')
                            matched += 1

                    print(f"{datetime.now()} For input #{progress_count}, found:{found} / matched:{matched}",
                        file=sys.stderr)
                else: 
                    print('no more results', file=sys.stderr)
                    break
            else:
                print(f'{r.status_code}', file=sys.stderr)
                print(f'{r.text}', file=sys.stderr)
                break

            if not request_url.endswith('/scroll'):
                break
