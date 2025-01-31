from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


def thread(rows, function):
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_row = {executor.submit(function, row): row for row in rows}

        for future in as_completed(future_to_row):
            try:
                future.result()
            except Exception as e:
                print(str(e))


def calculate_age(birthdate_str):
    if not birthdate_str:
        return ""

    try:
        birthdate = datetime.strptime(birthdate_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        birthdate = birthdate.replace(tzinfo=timezone.utc)

        current_date = datetime.now(timezone.utc)

        age = current_date.year - birthdate.year - \
            ((current_date.month, current_date.day)
             < (birthdate.month, birthdate.day))

        return f"{age}"

    except:
        return ""


def parse_query_params(path):
    query = path.split('?')[1] if '?' in path else ''

    params = dict(param.split('=')
                  for param in query.split('&') if '=' in param)

    return params


def get_state_code(state_name):
    states = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY"
    }

    # Get the state code, defaulting to None if not found
    return states.get(state_name, state_name)


def get_location_query(address, city, state, zip):
    location_parts = []

    if address:
        location_parts.append(address)
    if city:
        location_parts.append(city)
    if state:
        state_code = get_state_code(state)
        location_parts.append(state_code)
    if zip:
        location_parts.append(zip)

    location_query = " ".join(location_parts) if location_parts else ""

    return location_query


def get_search_query(profile):

    first_name = profile.get('first_name', "")
    last_name = profile.get('last_name', "")
    address = profile.get('address', "")
    city = profile.get('city', "")
    state = profile.get('state', "")
    zip = profile.get('zip', "")
    age = calculate_age(profile.get('birth_date', ""))
    gender = profile.get('gender', "")

    # Get Location Query
    location_query = get_location_query(address, city, state, zip)

    # Build Search Query
    search_parts = [f"Name: {first_name} {last_name}"]

    if location_query:
        search_parts.append(f"Location: {location_query}")
    if age:
        search_parts.append(f"Age: {age}")
    if gender:
        search_parts.append(f"Gender: {gender}")

    search_query = ", ".join(search_parts)

    return search_query
