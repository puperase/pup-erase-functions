from http.server import BaseHTTPRequestHandler
import os
import requests
import json
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))
openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        # Parse search parameters from query
        params = self.parse_query_params()

        if 'type' not in params or 'full_name' not in params:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Invalid parameters.')
            return
        
        search_type = params['type']
        full_name = params['full_name']

        # Split full name into first and last name
        name_parts = full_name.split(" ", 1)  # Limit to two parts
        if len(name_parts) < 2:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Full name must include both first and last name.')
            return
        
        first_name = name_parts[0]
        last_name = name_parts[1]

        if search_type == 'google':
            result = run_google(first_name, last_name)
        elif search_type == 'broker':
            result = run_brokers(first_name, last_name)
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Unknown search type.')
            return

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(result).encode('utf-8'))


    def parse_query_params(self):
        # Simple query parameter parsing
        query = self.path.split('?')[1] if '?' in self.path else ''
        params = dict(param.split('=') for param in query.split('&') if '=' in param)
        return params


def run_google(first_name, last_name):
    items = []
    
    start = 0
    while start < 100:
        scraping_url = f"https://www.googleapis.com/customsearch/v1?key={os.environ.get('GOOGLE_SEARCH_KEY')}&cx={os.environ.get('GOOGLE_SEARCH_ID')}&q={first_name} {last_name}&start={start}"
        print(f"Scraping URL: {scraping_url}")

        response = requests.get(scraping_url)
        data = response.json()

        for item in data.get('items', []):
            items.append(item)

        start += 10

    # Open AI processing
    person_info = json.dumps({
        'Name': f"{first_name} {last_name}",
        'Age': "N/A",  
        "Gender": "N/A",
        "Location": "N/A"
    })

    scores = []
    for item in items:
        chat_completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": f"Rate this search result for matching person {person_info}:\n\n{json.dumps(item)}\n\nProvide only the score (0-10) as a number, without any explanation or additional text."
            }]
        )
        response = json.loads(chat_completion.to_json())
        score = int(response['choices'][0]['message']['content'].strip())
        scores.append((item, score))

    scores.sort(key=lambda x: x[1], reverse=True)
    top_matches = [result[0] for result in scores[:20]]

    return {
        "source": "google",
        "results": top_matches
    }


def run_brokers(first_name, last_name):
    brokers_response = supabase.table("brokers").select("name, scraping_url").execute()
    
    broker_results = []

    for broker in brokers_response.data:
        profile_url = broker['scraping_url'].replace("{first_name}", first_name).replace("{last_name}", last_name)
        print(f"Scraping URL for {broker['name']}: {profile_url}")

        scraping_result = firecrawl.scrape_url(
            profile_url,
            params={
                'formats': ['extract'],
                "extract": {
                    "prompt": f"Extract the person's information from the page for {first_name} {last_name}."
                }
            }
        )

        if scraping_result.get('extract'):
            broker_results.append({
                "broker": broker['name'],
                "result": scraping_result['extract']
            })
        else:
            broker_results.append({
                "broker": broker['name'],
                "error": "No data found"
            })

    return {
        "source": "broker",
        "results": broker_results
    }
