import pandas as pd

old = 'domain_with_crawl_per_day.csv'
df = pd.read_csv(old)

crawls = df['crawls']

new = 'priority_domain_list_with_delays.csv'
dfnew = pd.read_csv(new)

dfnew['crawls'] = crawls

dfnew.to_csv(new, index=False)