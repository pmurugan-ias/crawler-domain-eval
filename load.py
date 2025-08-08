import csv
from urllib.robotparser import RobotFileParser
from func_timeout import func_timeout, FunctionTimedOut

def get_crawl_delay(domain: str, scheme: str = "http") -> float:
    url = f"{scheme}://{domain}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(url)
    try:
        rp.read()
        delay = rp.crawl_delay("ias-crawler")  # get crawl delay for user-agent '*'
        if not delay: 
            delay = rp.crawl_delay("*")
        if not delay:
            delay = 0.1 
        return float(delay)
    except Exception:
        return float(0.1)

def main():
    domains = []
    with open('priority_domain_list.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row:
                domains.append(row)

    print("here")
    updated_rows = []
    for row in domains: 
        print(row)
        domain = row[0]

        
        try: 
            http_delay = func_timeout(10, get_crawl_delay, args=(domain,), kwargs={"scheme": "http"})
        except FunctionTimedOut:
            print(f"Timeout on HTTP crawl delay for {domain}")
            http_delay = 0.1  # fallback default

        try: 
            https_delay = func_timeout(10, get_crawl_delay, args=(domain,), kwargs={"scheme": "https"})
        except FunctionTimedOut:
            print(f"Timeout on HTTPS crawl delay for {domain}")
            https_delay = 0.1  # fallback default


        updated_rows.append(row + [http_delay, https_delay])

    # Write result
    with open('priority_domain_list_with_delays.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['domain', 'http_delay', 'https_delay'])
        writer.writerows(updated_rows)

if __name__ == "__main__":
    main()
