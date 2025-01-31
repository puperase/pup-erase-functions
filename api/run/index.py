from http.server import BaseHTTPRequestHandler

import os
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI
from utils import helpers, scanners

# Initialize clients
supabase: Client = create_client(os.environ.get(
    "SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
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
    google_searches = supabase.table("google_searches").select(
        "*, profiles(*)").eq("search_status", "queued").execute().data

    for google_search in google_searches:
        supabase.table("google_searches") \
            .update({"search_status": 'in_progress'}) \
            .eq("id", google_search['id']) \
            .execute()

        profile = google_search['profiles']

        # Fetch 100 search results from Google API
        results = scanners.scan_google(profile)

        # Rank first 20 results
        top_matches = results[:20]

        # Save into Database
        supabase.table("google_searches") \
            .update({"search_result": top_matches, "search_status": 'completed'}) \
            .eq("id", google_search['id']) \
            .execute()


def run_brokers():

    def run_broker_scraping(broker_search):
        supabase.table("broker_searches") \
            .update({"search_status": 'in_progress'}) \
            .eq("id", broker_search['id']) \
            .execute()

        broker = broker_search['brokers']
        profile = broker_search['profiles']

        result = scanners.scan_broker(
            broker=broker,
            profile=profile
        )

        if result:
            supabase.table("broker_searches") \
                .update({"search_result": result, "search_status": 'completed'}) \
                .eq("id", broker_search['id']) \
                .execute()

        else:
            supabase.table("broker_searches") \
                .update({"search_status": 'failed', 'search_result': {'error': 'No data found'}}) \
                .eq("id", broker_search['id']) \
                .execute()

    # Array of Brokers to scrap
    broker_searches = supabase.table("broker_searches").select(
        "*, brokers(*), profiles(*)").eq("search_status", "queued").execute().data

    # Concurrent Thread
    helpers.thread(broker_searches[:30], run_broker_scraping)
