from http.server import BaseHTTPRequestHandler
from datetime import datetime
import os
import re
import json
from supabase import create_client, Client
from firecrawl import FirecrawlApp
from openai import OpenAI

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_KEY"))

openai = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)

class handler(BaseHTTPRequestHandler):

    def do_GET(self):

        response = supabase.table("jobs").select("*, broker(*), user(*)").execute()

        jobs = response.data
        for job in jobs:
            try:
                if job['status'] == "queued" and job['broker']['enable_scraping'] and job['broker']['scraping_url'] and job['broker']['scraping_selector']:
                    print(f"Checking {job['broker']['name']} for user {job['user']['first_name']}")

                    # FireCrawl
                    scraping_url = job['broker']['scraping_url']
                    scraping_url = scraping_url.replace("{first_name}", job['user']['first_name'])
                    scraping_url = scraping_url.replace("{last_name}", job['user']['last_name'])
                    scraping_url = scraping_url.replace("{name}", f"{job['user']['first_name']} {job['user']['last_name']}")
                    scraping_url = scraping_url.replace("{gender}", "Male" if job['user']['gender'] == 0 else "Female")
                    scraping_url = scraping_url.replace("{city}", job['user']['city'])
                    scraping_url = scraping_url.replace("{state}", job['user']['state'])
                    scraping_url = scraping_url.replace("{country}", "United States")
                    scraping_url = scraping_url.replace("{age}", f"{calculate_age(job['user']['birth_date'])}")

                    print(f"Scraping URL: {scraping_url}")

                    scrape_result = firecrawl.scrape_url(scraping_url, params={'formats': ['markdown']})

                    # Open API
                    chat_completion = openai.chat.completions.create(
                        messages=[
                            {
                                "role": "user",
                                "content": f"Extract the person's information from the following markdown and "
                                            f"structure it into JSON array format with fields: name, email, birthday, "
                                            f"phone number, and address.\n\n{scrape_result}\n",
                            }
                        ],
                        model="gpt-4o-mini",
                    )
                    response = json.loads(chat_completion.to_json())
                    response_text = response['choices'][0]['message']['content'].strip()

                    # Format Response
                    match = re.search(r'\[\s*{.*?}\s*\]', response_text, re.DOTALL)
                    if match:
                        person_info_json = match.group(0)
                        person_data = json.loads(person_info_json)
                        print(json.dumps(person_data, indent=4))

                        supabase.table("jobs") \
                            .update({"result": person_data, "status": 'completed'}) \
                            .eq("id", job['id']) \
                            .execute()

                    else:
                        print("No valid JSON found.")
            except Exception as e:
                print(e)
                continue

        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write('Completed!'.encode('utf-8'))
        return

def calculate_age(birthdate):
    try:
        today = datetime.today()
        age = today.year - birthdate.year
        if (today.month, today.day) < (birthdate.month, birthdate.day):
            age -= 1
        return age
    
    except:
        return 40
