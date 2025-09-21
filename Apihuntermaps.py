from flask import Flask
import gspread
import googlemaps
import time
import os
import json

# --- CONFIGURATION ---
MAPS_API_KEY = os.environ.get('MAPS_API_KEY')
SPREADSHEET_NAME = "Lead Gen Engine"
SEARCH_QUERY = "restaurants in Indiranagar Bengaluru"

app = Flask(__name__)

@app.route('/')
def run_hunter_script():
    try:
        gc = gspread.service_account(filename="gspread_credentials.json")
        leads_worksheet = gc.open(SPREADSHEET_NAME).worksheet("LEADS")
        existing_names = set(leads_worksheet.col_values(1))
    except Exception as e:
        return f"Error connecting to Google Sheets: {e}"

    try:
        gmaps = googlemaps.Client(key=MAPS_API_KEY)
    except Exception as e:
        return f"Error connecting to Google Maps API: {e}"

    try:
        places_result = gmaps.places(query=SEARCH_QUERY)
        results = places_result.get('results', [])
    except Exception as e:
        return f"Error during API search: {e}"

    leads_logged = 0
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
            area = SEARCH_QUERY.split(" in ")[1] if " in " in SEARCH_QUERY else "Unknown"
            
            leads_worksheet.append_row([name, rating, website, phone, "Not Found", "Pending", area, website if website != "No Website Found" else ""])
            existing_names.add(name)
            leads_logged += 1
        except Exception as e:
            print(f"Could not get details for {name}: {e}")
            continue
            
    final_message = f"Hunter run complete! Logged {leads_logged} new leads."
    return final_message

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
