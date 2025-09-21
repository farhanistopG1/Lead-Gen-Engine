# --- Filename: Apihuntermaps.py ---
from flask import Flask
import gspread
import googlemaps
import time
import os
import json
import sys

# --- CONFIGURATION ---
MAPS_API_KEY = os.environ.get('MAPS_API_KEY')
SPREADSHEET_NAME = "Lead Gen Engine"

app = Flask(__name__)

@app.route('/')
def run_hunter_script():
    # This script is now designed to be called by another script.
    # We will pass the search query in when we call it.
    # This Flask wrapper remains for manual testing if needed.
    return "Hunter is ready. Should be triggered by the Master script."

def find_leads(search_query):
    # This is the core logic, now in a reusable function
    try:
        gc = gspread.service_account(filename="gspread_credentials.json")
        leads_worksheet = gc.open(SPREADSHEET_NAME).worksheet("LEADS")
        existing_names = set(leads_worksheet.col_values(1))
        gmaps = googlemaps.Client(key=MAPS_API_KEY)
    except Exception as e:
        print(f"Error during initialization: {e}")
        return 0

    try:
        places_result = gmaps.places(query=search_query)
        # Add logic to handle subsequent pages of results if needed
        results = places_result.get('results', [])
    except Exception as e:
        print(f"Error during API search: {e}")
        return 0

    leads_logged = 0
    area = search_query.split(" in ")[1].split(" with")[0] if " in " in search_query else "Unknown"

    for place in results:
        name = place.get('name', 'Not Found')
        if name in existing_names:
            continue

        place_id = place.get('place_id')
        if not place_id:
            continue

        try:
            details = gmaps.place(place_id=place_id, fields=['website', 'formatted_phone_number', 'rating'])
            place_details = details.get('result', {})

            rating = place_details.get('rating', 'Not Found')
            website = place_details.get('website', 'No Website Found')
            phone = place_details.get('formatted_phone_number', 'Not Found')
            
            leads_worksheet.append_row([name, rating, website, phone, "Not Found", "Pending", area, website if website != "No Website Found" else ""])
            existing_names.add(name)
            leads_logged += 1
        except Exception as e:
            print(f"Could not get details for {name}: {e}")
    
    print(f"Hunter run complete! Logged {leads_logged} new leads for {area}.")
    return leads_logged

if __name__ == '__main__':
    # This allows the script to be run from the command line with an argument
    if len(sys.argv) > 1:
        find_leads(sys.argv[1])
    else:
        # This part runs the web server if no command-line argument is given
        app.run(host='0.0.0.0', port=10000)
