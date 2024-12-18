from http.server import BaseHTTPRequestHandler

import os
import requests
import json
import time
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI
from utils import helpers

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))
openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


class handler(BaseHTTPRequestHandler):

    def do_GET(self):

        run_google()
        run_brokers()

        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write('Completed!'.encode('utf-8'))
        return


def run_google():
    response = supabase.table("searches").select("*, profiles(*)").eq("broker_type", "google").eq("search_status", "queued").execute()

    for google in response.data:
        supabase.table("searches") \
            .update({"search_status": 'in_progress'}) \
            .eq("id", google['id']) \
            .execute()

        profile = google['profiles']

        try:
            if google['search_status'] == "queued":
                print(f"Checking Google for user {profile['first_name']}")

                # Google Search API
                items = []

                start = 0
                while start < 100:
                    scraping_url = f"https://www.googleapis.com/customsearch/v1?key={os.environ.get('GOOGLE_SEARCH_KEY')}&cx={os.environ.get('GOOGLE_SEARCH_ID')}&q={profile['first_name']} {profile['last_name']}&start={start}"
                    print(f"Scraping URL: {scraping_url}")

                    response = requests.get(scraping_url)
                    data = response.json()

                    for item in data.get('items', []):
                        items.append(item)

                    start += 10

                # Open API
                person_info = json.dumps({
                    'Name': f"{profile['first_name']} {profile['last_name']}",
                    'Age': helpers.calculate_age(profile['birth_date']),
                    "Gender": profile['gender'],
                    "Location": f"{profile['city']}, {profile['state']}, United States"
                })

                scores = []
                for item in items:
                    chat_completion = openai.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {
                                "role": "user",
                                "content": f"Rate this search result for matching person {person_info}:\n\n{json.dumps(item)}\n\nProvide only the score (0-10) as a number, without any explanation or additional text."
                            }
                        ],
                    )
                    response = json.loads(chat_completion.to_json())
                    score = int(response['choices'][0]['message']['content'].strip())

                    scores.append((item, score))

                scores.sort(key=lambda x: x[1], reverse=True)
                top_matches = [result[0] for result in scores[:20]]

                supabase.table("searches") \
                    .update({"search_result": top_matches, "search_status": 'completed'}) \
                    .eq("id", google['id']) \
                    .execute()

        except Exception as e:
            print(e)
            supabase.table("searches") \
                .update({"search_status": 'failed', 'search_result': {'error': str(e)}}) \
                .eq("id", google['id']) \
                .execute()


def run_brokers():
    response = supabase.table("searches").select("*, brokers(*), profiles(*)").eq("broker_type", "broker_site").eq("search_status", "queued").execute()

    for search in response.data:
        supabase.table("searches") \
            .update({"search_status": 'in_progress'}) \
            .eq("id", search['id']) \
            .execute()
        
        broker = search['brokers']
        profile = search['profiles']

        try:
            if search['search_status'] == "queued" and broker['enable_scraping'] and broker['scraping_url']:
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
                    supabase.table("searches") \
                        .update({"search_result": scraping_result['extract'], "search_status": 'completed'}) \
                        .eq("id", search['id']) \
                        .execute()
                    
                else:
                    supabase.table("searches") \
                        .update({"search_status": 'failed', 'search_result': {'error': 'No data found'}}) \
                        .eq("id", search['id']) \
                        .execute()

        except Exception as e:
            print(e)
            supabase.table("searches") \
                .update({"search_status": 'failed', 'search_result': {'error': str(e)}}) \
                .eq("id", search['id']) \
                .execute()


if __name__ == '__main__':
    while True:
        run_google()
        run_brokers()

        time.sleep(60)
