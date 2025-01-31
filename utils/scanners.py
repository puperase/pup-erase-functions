import os
import requests
import json
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI
from utils import helpers

supabase: Client = create_client(os.environ.get(
    "SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))
openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def find_top_matches(results, profile):
    # Open AI processing setup
    search_query = helpers.get_search_query(profile)

    # Create a prompt with all results
    prompt = (
        f"Sort the following search results by relevance for the person {search_query}. \n\n" +
        "Return only the sorted list of indices (starting from 0) of the results in descending order of relevance:\n\n" +
        "\n".join([f"{i}: {json.dumps(result)}" for i,
                  result in enumerate(results)])
    )

    try:
        chat_completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        response = json.loads(chat_completion.to_json())
        data = response['choices'][0]['message']['content']
        sorted_indices = json.loads(data)

        # Extract top 20 results based on sorted indices
        top_matches = [results[i] for i in sorted_indices[:20]]
        return top_matches
    except Exception as e:
        print(f"Error with OpenAI API: {e}")
        return []


def scan_google(profile):

    results = []

    # Search Function
    def run_google_api(scraping_url):
        print(f"Google Search: {scraping_url}")

        response = requests.get(scraping_url)
        data = response.json()

        for item in data.get('items', []):
            results.append(item)

    # Build array of scraping URLs
    search_query = helpers.get_search_query(profile)

    scraping_urls = []
    for start in range(0, 100, 10):
        scraping_url = f"""https://www.googleapis.com/customsearch/v1?key={os.environ.get('GOOGLE_SEARCH_KEY')}&cx={
            os.environ.get('GOOGLE_SEARCH_ID')}&q={search_query}&start={start}"""
        scraping_urls.append(scraping_url)

    # Concurrent Thread
    helpers.thread(scraping_urls, run_google_api)

    # Return all searched results
    return results


def scan_broker(broker, profile):

    # Build Scraping URL
    first_name = profile.get('first_name', "")
    last_name = profile.get('last_name', "")
    city = profile.get('city', "")
    state = profile.get('state', "")
    zip = profile.get('zip', "")
    age = helpers.calculate_age(profile.get('birth_date'))

    scraping_url = str(broker['scraping_url']).replace("{name}", f"{first_name} {last_name}").replace("{first_name}", first_name).replace(
        "{last_name}", last_name).replace("{city}", city).replace("{state}", state).replace("{state_code}", helpers.get_state_code(state)).replace("{zip}", zip).replace("{age}", age)

    # Build Search Query
    search_query = helpers.get_search_query(profile)

    # Search Function
    print(f"Broker Search: {scraping_url}")

    try:
        scraping_result = firecrawl.scrape_url(
            scraping_url,
            params={
                'formats': ['extract'],
                "extract": {
                    "prompt": f"Extract the person's information from the page for {search_query}. Just limit the results up to 3 arraies of structures objects for First Name, Last Name, Address, Phone, Email, Gender, Birthday, Family relations, and Other Metadata."
                },
                "timeout": 15000
            }
        )

        if scraping_result.get('extract'):
            return scraping_result['extract']
        else:
            return None

    except Exception as e:
        print(e)
        return None
