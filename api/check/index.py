from http.server import BaseHTTPRequestHandler
import os
import json
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI
from utils import helpers, scanners

# Initialize clients
supabase: Client = create_client(os.environ.get(
    "SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))
openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        params = helpers.parse_query_params(self.path)

        if 'type' not in params or 'first_name' not in params or 'last_name' not in params:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Invalid parameters.')
            return

        search_type = params['type']
        profile = {
            'first_name': params['first_name'],
            'last_name': params['last_name'],
            'city': params.get('city', ""),
            'state': params.get('state', ""),
            'zip': params.get('zip', "")
        }

        if search_type == 'google':
            results = run_google(profile)
        elif search_type == 'broker':
            results = run_brokers(profile)
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Unknown search type.')
            return

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode('utf-8'))


def run_google(profile):
    # Fetch 100 search results from Google API
    results = scanners.scan_google(profile)

    # Rank first 20 results
    top_matches = results[:20]

    # Return
    return top_matches


def run_brokers(profile):
    results = []

    def run_broker_scraping(broker):
        result = scanners.scan_broker(broker, profile)

        if result:
            results.append({
                "search_status": "completed",
                "broker": broker,
                "search_result": result
            })
        else:
            results.append({
                "search_status": "failed",
                "broker": broker,
            })

    # Array of Brokers to scrap
    brokers = supabase.table("brokers").select(
        "name, scraping_url").eq("enable_scraping", True).execute().data

    # Dev test Loop
    # for broker in brokers[:30]:
    #     run_broker_scraping(broker)

    # Concurrent Thread
    helpers.thread(brokers[:30], run_broker_scraping)

    # Return all scanned results
    return results
