from http.server import BaseHTTPRequestHandler

import os
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from utils import helpers

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))


class handler(BaseHTTPRequestHandler):

    def do_GET(self):

        run_process()

        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write('Completed!'.encode('utf-8'))
        return
    

def run_process():
    response = supabase.table("search").select("*, brokers(*), profiles(*)").eq("status", "queued").execute()

    for search in response.data:
        supabase.table("search") \
            .update({"status": 'in_progress'}) \
            .eq("id", search['id']) \
            .execute()
        
        broker = search['brokers']
        profile = search['profiles']

        try:
            if search['status'] == "queued" and broker['enable_scraping'] and broker['scraping_url'] and broker['scraping_selector']:
                print(f"Checking {broker['name']} for user {profile['first_name']}")

                # FireCrawl
                scraping_url = broker['scraping_url']
                scraping_url = scraping_url.replace(
                    "{first_name}", profile['first_name'])
                scraping_url = scraping_url.replace(
                    "{last_name}", profile['last_name'])
                scraping_url = scraping_url.replace("{name}", f"{profile['first_name']} {profile['last_name']}")
                scraping_url = scraping_url.replace(
                    "{gender}", "Male" if profile['gender'] == 0 else "Female")
                scraping_url = scraping_url.replace(
                    "{city}", profile['city'])
                scraping_url = scraping_url.replace(
                    "{state}", profile['state'])
                scraping_url = scraping_url.replace(
                    "{country}", "United States")
                scraping_url = scraping_url.replace(
                    "{age}", f"{helpers.calculate_age(profile['birth_date'])}")

                print(f"Scraping URL: {scraping_url}")

                scraping_result = firecrawl.scrape_url(
                    scraping_url, 
                    params={
                        'formats': ['extract'],
                        "extract": {
                            "prompt": f"Extract the person's information from the page and organize it into a JSON object with the following fields: name, email, birthday, phone_number, and address. If multiple entries are found, select the one that most closely matches."
                        }
                    })

                print(scraping_result)

                if scraping_result.get('extract', {}).get('name'):
                    supabase.table("search") \
                        .update({"result": scraping_result, "status": 'completed'}) \
                        .eq("id", search['id']) \
                        .execute()
                    
                else:
                    supabase.table("search") \
                        .update({"status": 'failed'}) \
                        .eq("id", search['id']) \
                        .execute()

        except Exception as e:
            print(e)
            supabase.table("search") \
                .update({"status": 'failed'}) \
                .eq("id", search['id']) \
                .execute()


if __name__ == '__main__':
    print("run process")
    run_process()
