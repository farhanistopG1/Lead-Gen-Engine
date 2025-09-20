# === Step 1: Define the entire Python script as a text string ===

script_code = """
import gspread
import googlemaps
import time

# --- CONFIGURATION ---
# Paste the new API key you just created for the Maps API
MAPS_API_KEY = "AIzaSyAFEkAM6hSaXQg-ofipKlrtO_RmpK4qAW8"

# These are from our previous setup
KEY_FILE_NAME = "new-mac-project-5bf7c5fa874a.json" # Change this
SPREADSHEET_NAME = "Lead Gen Engine"
SEARCH_QUERY = "restaurants in Indiranagar Bengaluru"

def main():
    # --- Part 1: Connect to Google Sheets ---
    try:
        print("Authenticating with Google Sheets...")
        gc = gspread.service_account(filename=KEY_FILE_NAME)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        print("✅ Successfully connected to 'LEADS' sheet.")
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return

    # --- Part 2: Connect to Google Maps API ---
    try:
        print("Connecting to Google Maps API...")
        gmaps = googlemaps.Client(key=MAPS_API_KEY)
        print("✅ Successfully connected to Google Maps API.")
    except Exception as e:
        print(f"❌ Error connecting to Google Maps API: {e}")
        return

    existing_names = set(leads_worksheet.col_values(1))
    
    # --- Part 3: Search for Places ---
    print(f"\\n--- Searching for '{SEARCH_QUERY}' using the Places API ---")
    try:
        places_result = gmaps.places(query=SEARCH_QUERY)
        results = places_result.get('results', [])
        print(f"Found {len(results)} potential leads in the first search.")
    except Exception as e:
        print(f"❌ Error during API search: {e}")
        return

    # --- Part 4: Get Details and Log to Sheet ---
    for place in results:
        name = place.get('name', 'Not Found')
        
        if name in existing_names:
            print(f"Skipping duplicate: {name}")
            continue

        place_id = place.get('place_id')
        if not place_id:
            continue

        try:
            print(f"-> Getting details for: {name}...")
            # Make a second API call to get specific details
            details = gmaps.place(place_id=place_id, fields=['website', 'formatted_phone_number', 'rating'])
            place_details = details.get('result', {})

            rating = place_details.get('rating', 'Not Found')
            website = place_details.get('website', 'No Website Found')
            phone = place_details.get('formatted_phone_number', 'Not Found')
            
            # Log the complete, enriched lead to the sheet
            leads_worksheet.append_row([name, rating, website, phone, "Not Found", "Pending", SEARCH_QUERY.split(" in ")[1], website if website != "No Website Found" else ""])
            existing_names.add(name)
            print(f"   ✅ Logged to sheet: {name}")
            time.sleep(1) # Be respectful to the API and avoid hitting rate limits

        except Exception as e:
            print(f"   ❌ Could not get details for {name}. Error: {e}")
            continue

    print("\\n🎉 'API Hunter' script finished successfully!")

main()
"""

# === Step 2: Write the script to a file ===
with open("api_hunter.py", "w") as file:
    file.write(script_code)
print("✅ Script file 'api_hunter.py' created successfully.")

# === Step 3: Execute the script ===
print("\n--- EXECUTING SCRIPT ---\n")
!python3 api_hunter.py