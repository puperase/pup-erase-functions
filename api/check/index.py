from http.server import BaseHTTPRequestHandler
import os
import requests
import json
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI

# Initialize clients
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))
openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        # Parse search parameters from query
        params = self.parse_query_params()

        if 'type' not in params or 'first_name' not in params or 'last_name' not in params:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Invalid parameters.')
            return
        
        search_type = params['type']
        first_name = params['first_name']
        last_name = params['last_name']
        location = params.get('location', None)  # Optional location parameter

        if search_type == 'google':
            result = run_google(first_name, last_name, location)  # Pass location
        elif search_type == 'broker':
            result = run_brokers(first_name, last_name, location)  # Pass location
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


def run_google(first_name, last_name, location=None):
    items = []
    
    start = 0
    while start < 20:
        # Include location in the search query if provided
        location_query = f" {location}" if location else ""
        scraping_url = f"https://www.googleapis.com/customsearch/v1?key={os.environ.get('GOOGLE_SEARCH_KEY')}&cx={os.environ.get('GOOGLE_SEARCH_ID')}&q={first_name} {last_name}{location_query}&start={start}"
        print(f"Scraping URL: {scraping_url}")

        response = requests.get(scraping_url)
        data = response.json()

        for item in data.get('items', []):
            items.append(item)

        start += 10

    # Open AI processing
    person_info = json.dumps({
        'Name': f"{first_name} {last_name}",
        'Location': location if location else "N/A",  # Include location in person info
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


def run_brokers(first_name, last_name, location=None):
    brokers_response = supabase.table("brokers").select("name, scraping_url").eq("enable_scraping", True).execute()
    
    broker_results = []
    max_brokers = 5

    for broker in brokers_response.data:
        if len(broker_results) >= max_brokers:  # Check if we have reached the limit
            break
        
        try:
            profile_url = broker['scraping_url'].replace("{first_name}", first_name).replace("{last_name}", last_name)
            print(f"Scraping URL for {broker['name']}: {profile_url}")

            scraping_result = firecrawl.scrape_url(
                profile_url,
                params={
                    'formats': ['extract'],
                    "extract": {
                        "prompt": f"Extract the person's information from the page for {first_name} {last_name} {'in ' + location if location else ''}."
                    },
                    "timeout": 10000
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
        except Exception as e:
            print(f"Error processing broker {broker['name']}: {e}")  # Log any errors that occur
            continue

    return {
        "source": "broker",
        "results": broker_results
    }

