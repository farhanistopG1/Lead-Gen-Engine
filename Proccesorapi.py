from flask import Flask
from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import json
import re
import time

# --- CONFIGURATION ---
# The script will now securely read your keys from Render's Environment Variables
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GSPREAD_JSON_STRING = os.environ.get('GSPREAD_JSON')

# Convert the JSON string from secrets into a dictionary
GSPREAD_CREDENTIALS = json.loads(GSPREAD_JSON_STRING)

SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3

# Create the Flask web server app
app = Flask(__name__)

# This is our main "start button" URL
@app.route('/')
def run_processor_script():
    # --- Part 1: Connect to Google Sheets & Find Tasks ---
    try:
        print("Authenticating with Google Sheets...")
        gc = gspread.service_account_from_dict(GSPREAD_CREDENTIALS)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
        print("✅ Successfully connected to Google Sheets.")
    except Exception as e:
        return f"❌ Error connecting to Google Sheets: {e}"

    all_leads = leads_worksheet.get_all_records()
    leads_to_process = [lead for lead in all_leads if str(lead.get('Status', '')).strip() == 'Pending']
    
    if not leads_to_process:
        return "✅ No pending leads found."
        
    print(f"Found {len(leads_to_process)} pending leads. Processing up to {MAX_LEADS_PER_RUN}.")
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    processed_count = 0
    for lead in leads_to_process:
        if processed_count >= MAX_LEADS_PER_RUN:
            break

        restaurant_name = lead['Restaurant Name']
        target_url = lead['Website URL']
        # Find the row number by searching for the unique restaurant name
        try:
            cell = leads_worksheet.find(restaurant_name)
            target_row_number = cell.row
        except gspread.exceptions.CellNotFound:
            print(f"Could not find row for {restaurant_name}, skipping.")
            continue
            
        print(f"\\n--- Processing Lead: {restaurant_name} (Row {target_row_number}) ---")

        # --- Part 2: Scrape the Website ---
        body_html = ""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                print(f"Navigating to {target_url}...")
                page.goto(target_url, timeout=60000)
                body_html = page.locator('body').inner_html()
                print("✅ Successfully downloaded website HTML.")
            except Exception as e:
                print(f"❌ Error scraping {target_url}: {e}")
                leads_worksheet.update_cell(target_row_number, 6, "Processing Error - Scraping Failed")
                continue # Move to the next lead in the batch
            finally:
                browser.close()

        # --- Part 3: The AI "Chain" ---
        try:
            # --- Call 1: The Analyst ---
            print("--- Sending to AI Analyst for Data Extraction & Flaw Analysis ---")
            prompt1 = f\"\"\"
            Analyze the raw HTML of {target_url}. Perform two tasks:
            TASK 1: Extract Data (About Us, Phone, Email, Social Media).
            TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
            HTML: {body_html}
            \"\"\"
            response1 = model.generate_content(prompt1)
            flaw_analysis = response1.text
            print("✅ Flaw analysis complete.")
            
            # --- Call 2: The Prompt Engineer ---
            print("--- Sending Analysis to AI Prompt Engineer ---")
            prompt2 = f"Based on the following website analysis, generate a detailed, high-quality prompt for an AI website builder like Lovable. ANALYSIS: {flaw_analysis}"
            response2 = model.generate_content(prompt2)
            builder_prompt = response2.text
            print("✅ Builder prompt generated.")

        except Exception as e:
            print(f"❌ An error occurred during the AI chain: {e}")
            leads_worksheet.update_cell(target_row_number, 6, "Processing Error - AI Failed")
            continue

        # --- Part 4: Log to RESULTS Sheet & Update LEADS Status ---
        try:
            print("--- Logging analysis to 'RESULTS' sheet... ---")
            if not results_worksheet.get_all_values():
                 results_worksheet.append_row(["Restaurant Name", "Flaw Analysis", "Builder Prompt"])
            results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt])
            
            print("--- Updating status in 'LEADS' sheet... ---")
            leads_worksheet.update_cell(target_row_number, 6, "Analysis Complete")
            print("✅ Sheets successfully updated!")
            processed_count += 1
        except Exception as e:
            print(f"❌ Error updating Sheets: {e}")

    return f"🎉 Full batch process complete! Processed {processed_count} leads."

# This part runs the web server when Render executes the Start Command
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
