# --- Filename: processor_api.py (Final Version) ---
from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import time

# --- CONFIGURATION ---
# IMPORTANT: Paste your Gemini API Key here
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY_HERE"
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3 # How many leads to process each time the script runs

# --- CONNECT TO SERVICES ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    leads_worksheet = spreadsheet.worksheet("LEADS")
    results_worksheet = spreadsheet.worksheet("RESULTS")
except Exception as e:
    print(f"FATAL: Error connecting to Google Sheets: {e}")
    exit(1)

try:
    genai.configure(api_key=GEMINI_API_KEY)
    # Using the stable 'latest' version of the flash model
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
except Exception as e:
    print(f"FATAL: Error configuring Gemini client. Is your API key correct? Error: {e}")
    exit(1)

# --- GET PENDING LEADS ---
try:
    all_leads = leads_worksheet.get_all_records()
    leads_to_process = [lead for lead in all_leads if str(lead.get("Status")).strip() == "Pending"]
except Exception as e:
    print(f"FATAL: Could not read leads from Google Sheet: {e}")
    exit(1)

if not leads_to_process:
    print("No pending leads found. Exiting.")
    exit(0)

print(f"Found {len(leads_to_process)} pending leads. Processing up to {MAX_LEADS_PER_RUN}.")
processed_count = 0

# --- PROCESS LEADS ---
for lead in leads_to_process:
    if processed_count >= MAX_LEADS_PER_RUN:
        break

    restaurant_name = lead.get("Restaurant Name")
    target_url = lead.get("Website URL", "").strip()
    
    # Find the correct row number for the current lead to ensure status updates work
    try:
        cell = leads_worksheet.find(restaurant_name)
        target_row_number = cell.row
    except gspread.exceptions.CellNotFound:
        print(f"Warning: Could not find row for '{restaurant_name}', skipping.")
        continue

    # Check for a valid URL before trying to scrape
    if not target_url or not target_url.startswith('http'):
        print(f"Invalid URL for '{restaurant_name}'. Marking as failed.")
        leads_worksheet.update_cell(target_row_number, 6, "Processing Error - No Website")
        continue

    print(f"--- Processing Lead: {restaurant_name} (Row: {target_row_number}) ---")

    # ---- SCRAPE WEBSITE ----
    body_html = ""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(target_url, timeout=60000)
            body_html = page.locator("body").inner_html()
            browser.close()
    except Exception as e:
        print(f"Error scraping {target_url}: {e}")
        leads_worksheet.update_cell(target_row_number, 6, "Processing Error - Scraping Failed")
        continue

    # ---- AI ANALYSIS ----
    try:
        prompt1 = f"Analyze the raw HTML of the website for '{restaurant_name}'. Perform two tasks: TASK 1: Extract Data (About Us, Phone, Email, Social Media). TASK 2: Provide a Strategic Analysis of 3 critical website flaws. HTML: {body_html}"
        response1 = model.generate_content(prompt1)
        flaw_analysis = response1.text

        prompt2 = f"Based on the following website analysis for '{restaurant_name}', generate a detailed prompt for an AI website builder. ANALYSIS: {flaw_analysis}"
        response2 = model.generate_content(prompt2)
        builder_prompt = response2.text

        # ---- LOG TO SHEETS ----
        results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt, "", ""])
        leads_worksheet.update_cell(target_row_number, 6, "Analysis Complete")
        
        processed_count += 1
        print(f"✅ Successfully processed {restaurant_name}")
        time.sleep(1) # Small delay to respect API rate limits
    except Exception as e:
        print(f"Error during AI chain or sheet update for {restaurant_name}: {e}")
        leads_worksheet.update_cell(target_row_number, 6, "Processing Error - AI Failed")
        continue

print(f"\nProcessor run complete! Processed {processed_count} lead(s).")
