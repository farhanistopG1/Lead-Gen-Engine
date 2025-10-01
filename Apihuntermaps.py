"""
ROBUST API HUNTER - Lead Gen Engine v2.0
=========================================
Features:
- Pagination support (up to 60 results per search)
- Smart duplicate detection
- Better error handling and logging
- Configurable lead targets
- Rate limiting protection
- Detailed statistics
"""

from flask import Flask, jsonify
import gspread
import googlemaps
import time
import os
import sys
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Keys - OPTION 1: Environment variable (RECOMMENDED)
MAPS_API_KEY = os.environ.get('MAPS_API_KEY')

# API Keys - OPTION 2: Hardcoded (for testing only - remove before deploying)
if not MAPS_API_KEY:
    MAPS_API_KEY = "AIzaSyAFEkAM6hSaXQg-ofipKlrtO_RmpK4qAW8"  # Replace with your key

SPREADSHEET_NAME = "Lead Gen Engine"
CAMPAIGNS_SHEET = "CAMPAIGNS"
LEADS_SHEET = "LEADS"

# Pagination settings
MAX_RESULTS_PER_SEARCH = 60  # Google allows up to 60 (3 pages of 20)
PAGE_DELAY = 2  # Seconds to wait between page requests (required by Google)

# Rate limiting
REQUEST_DELAY = 1  # Delay between individual API calls

# ============================================================================
# FLASK APP SETUP
# ============================================================================

app = Flask(__name__)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def log(message, level="INFO"):
    """Enhanced logging with timestamps"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def normalize_name(name):
    """Normalize restaurant name for better duplicate detection"""
    if not name:
        return ""
    # Remove common suffixes and normalize
    name = name.lower().strip()
    # Remove punctuation
    name = ''.join(c for c in name if c.isalnum() or c.isspace())
    return name

def get_area_from_query(query):
    """Extract area name from search query"""
    # Handle queries like "South Indian restaurants in Indiranagar Bengaluru"
    if " in " in query:
        return query.split(" in ")[1].split(" with")[0].strip()
    return "Unknown"

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def connect_to_sheets():
    """Connect to Google Sheets with error handling"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        
        if not os.path.exists(creds_path):
            log("ERROR: gspread_credentials.json not found!", "ERROR")
            return None, None, None
        
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        
        campaigns_sheet = spreadsheet.worksheet(CAMPAIGNS_SHEET)
        leads_sheet = spreadsheet.worksheet(LEADS_SHEET)
        
        log(f"✅ Connected to spreadsheet: {SPREADSHEET_NAME}")
        return gc, campaigns_sheet, leads_sheet
        
    except Exception as e:
        log(f"Failed to connect to Google Sheets: {e}", "ERROR")
        return None, None, None

def get_existing_leads(leads_sheet):
    """Get all existing lead names for duplicate checking"""
    try:
        # Get all names from column A
        all_names = leads_sheet.col_values(1)
        # Normalize and create a set for fast lookup
        existing = set()
        for name in all_names[1:]:  # Skip header
            if name.strip():
                existing.add(normalize_name(name))
        
        log(f"Loaded {len(existing)} existing leads from database")
        return existing
        
    except Exception as e:
        log(f"Error loading existing leads: {e}", "WARNING")
        return set()

# ============================================================================
# GOOGLE MAPS API FUNCTIONS
# ============================================================================

def search_places_with_pagination(gmaps, query):
    """
    Search Google Places API with pagination support
    Returns up to 60 results (3 pages of 20)
    """
    all_results = []
    
    try:
        log(f"🔍 Searching: {query}")
        
        # First page
        places_result = gmaps.places(query=query)
        results = places_result.get('results', [])
        all_results.extend(results)
        log(f"   Page 1: Found {len(results)} results")
        
        # Check for additional pages
        page_count = 1
        while 'next_page_token' in places_result and len(all_results) < MAX_RESULTS_PER_SEARCH:
            page_count += 1
            
            # IMPORTANT: Google requires a short delay before using page token
            log(f"   Waiting {PAGE_DELAY}s before fetching page {page_count}...")
            time.sleep(PAGE_DELAY)
            
            try:
                page_token = places_result['next_page_token']
                places_result = gmaps.places(query=query, page_token=page_token)
                results = places_result.get('results', [])
                all_results.extend(results)
                log(f"   Page {page_count}: Found {len(results)} results")
                
            except Exception as e:
                log(f"   Error fetching page {page_count}: {e}", "WARNING")
                break
        
        log(f"✅ Total results: {len(all_results)} places")
        return all_results
        
    except Exception as e:
        log(f"Error during API search: {e}", "ERROR")
        return []

def get_place_details(gmaps, place_id, place_name):
    """Get detailed information for a specific place"""
    try:
        details = gmaps.place(
            place_id=place_id,
            fields=['website', 'formatted_phone_number', 'rating', 'formatted_address']
        )
        return details.get('result', {})
        
    except Exception as e:
        log(f"   Could not get details for {place_name}: {e}", "WARNING")
        return {}

# ============================================================================
# LEAD PROCESSING
# ============================================================================

def process_and_save_leads(places, existing_names, leads_sheet, area):
    """
    Process places and save to Google Sheets
    Returns: (new_leads_count, duplicate_count, error_count)
    """
    new_leads = 0
    duplicates = 0
    errors = 0
    
    gmaps = googlemaps.Client(key=MAPS_API_KEY)
    
    for i, place in enumerate(places, 1):
        try:
            # Extract basic info
            name = place.get('name', 'Not Found')
            if not name or name == 'Not Found':
                errors += 1
                continue
            
            # Check for duplicates
            normalized_name = normalize_name(name)
            if normalized_name in existing_names:
                duplicates += 1
                log(f"   [{i}/{len(places)}] SKIP: {name} (duplicate)")
                continue
            
            # Get place ID
            place_id = place.get('place_id')
            if not place_id:
                errors += 1
                log(f"   [{i}/{len(places)}] SKIP: {name} (no place_id)")
                continue
            
            log(f"   [{i}/{len(places)}] Processing: {name}")
            
            # Get detailed information
            place_details = get_place_details(gmaps, place_id, name)
            
            rating = place_details.get('rating', place.get('rating', 'Not Found'))
            website = place_details.get('website', 'No Website Found')
            phone = place_details.get('formatted_phone_number', 'Not Found')
            address = place_details.get('formatted_address', place.get('formatted_address', 'Not Found'))
            
            # Prepare row data
            # Columns: Restaurant Name, Rating, Website, Phone Number, Email, Status, Area, Website URL
            row_data = [
                name,
                rating,
                website,
                phone,
                "Not Found",  # Email (to be filled later)
                "Pending",    # Status
                area,
                website if website != "No Website Found" else ""
            ]
            
            # Save to sheet
            leads_sheet.append_row(row_data)
            existing_names.add(normalized_name)
            new_leads += 1
            
            log(f"   ✅ Added: {name}")
            
            # Rate limiting
            time.sleep(REQUEST_DELAY)
            
        except Exception as e:
            errors += 1
            log(f"   ❌ Error processing place: {e}", "ERROR")
            continue
    
    return new_leads, duplicates, errors

# ============================================================================
# MAIN HUNTER FUNCTION
# ============================================================================

def run_hunter(search_query=None):
    """
    Main function to find and log leads
    If search_query is provided, uses that. Otherwise, gets from command line.
    """
    stats = {
        'query': search_query,
        'places_found': 0,
        'new_leads': 0,
        'duplicates': 0,
        'errors': 0,
        'status': 'success'
    }
    
    log("="*70)
    log("🚀 STARTING ROBUST API HUNTER v2.0")
    log("="*70)
    
    # Validate API key
    if not MAPS_API_KEY or MAPS_API_KEY == "YOUR_GOOGLE_MAPS_API_KEY":
        log("ERROR: Google Maps API key is missing or invalid!", "ERROR")
        stats['status'] = 'error'
        stats['message'] = 'Invalid API key'
        return stats
    
    # Connect to Google Sheets
    gc, campaigns_sheet, leads_sheet = connect_to_sheets()
    if not gc or not leads_sheet:
        stats['status'] = 'error'
        stats['message'] = 'Failed to connect to Google Sheets'
        return stats
    
    # Get existing leads
    existing_names = get_existing_leads(leads_sheet)
    
    # Initialize Google Maps client
    try:
        gmaps = googlemaps.Client(key=MAPS_API_KEY)
        log("✅ Google Maps API initialized")
    except Exception as e:
        log(f"Failed to initialize Google Maps API: {e}", "ERROR")
        stats['status'] = 'error'
        stats['message'] = str(e)
        return stats
    
    # Get search query
    if not search_query:
        log("No search query provided", "ERROR")
        stats['status'] = 'error'
        stats['message'] = 'No search query'
        return stats
    
    # Extract area name
    area = get_area_from_query(search_query)
    log(f"📍 Target area: {area}")
    
    # Search with pagination
    places = search_places_with_pagination(gmaps, search_query)
    stats['places_found'] = len(places)
    
    if not places:
        log("⚠️  No places found", "WARNING")
        stats['status'] = 'warning'
        stats['message'] = 'No places found'
        return stats
    
    # Process and save leads
    log(f"\n📊 Processing {len(places)} places...")
    new_leads, duplicates, errors = process_and_save_leads(
        places, existing_names, leads_sheet, area
    )
    
    stats['new_leads'] = new_leads
    stats['duplicates'] = duplicates
    stats['errors'] = errors
    
    # Final summary
    log("="*70)
    log("📈 HUNTER RUN COMPLETE - SUMMARY:")
    log("="*70)
    log(f"Search Query: {search_query}")
    log(f"Area: {area}")
    log(f"Places Found: {stats['places_found']}")
    log(f"✅ New Leads Added: {new_leads}")
    log(f"🔄 Duplicates Skipped: {duplicates}")
    log(f"❌ Errors: {errors}")
    log("="*70)
    
    return stats

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def home():
    """Home route - displays status"""
    return jsonify({
        'status': 'ready',
        'message': 'Robust API Hunter v2.0 is running',
        'features': [
            'Pagination support (up to 60 results)',
            'Smart duplicate detection',
            'Enhanced error handling',
            'Detailed logging'
        ]
    })

@app.route('/hunt/<path:query>')
def hunt_with_query(query):
    """Hunt for leads with a specific query via URL"""
    stats = run_hunter(query)
    return jsonify(stats)

# ============================================================================
# COMMAND LINE EXECUTION
# ============================================================================

if __name__ == '__main__':
    # Check if running from command line with arguments
    if len(sys.argv) > 1:
        # Command line mode: python Apihuntermaps_v2_robust.py "restaurants in Indiranagar"
        search_query = sys.argv[1]
        run_hunter(search_query)
    else:
        # Flask web server mode
        log("Starting Flask web server...")
        log("To run from command line: python Apihuntermaps_v2_robust.py 'your search query'")
        app.run(host='0.0.0.0', port=10000, debug=False)
