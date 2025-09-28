
from flask import Flask
from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import json
import re
import time

# --- CONFIGURATION ---
# FIX 1: Add your Gemini API key directly here
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3

app = Flask(__name__)

@app.route('/')
def run_processor_script():
    # --- Part 1: Connect to Google Sheets & Find Tasks ---
    try:
        # FIX 2: Use an absolute path for the credentials file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
    except Exception as e:
        return f"Error connecting to Google Sheets: {e}"

    all_leads = leads_worksheet.get_all_records()
    leads_to_process = [lead for lead in all_leads if str(lead.get('Status')).strip() == 'Pending']
    
    if not leads_to_process:
        return "No pending leads found."
        
    # Configure the Gemini client
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except Exception as e:
        return f"Error configuring Gemini client. Is your API key correct? Error: {e}"
    
    processed_count = 0
    for lead in leads_to_process:
        if processed_count >= MAX_LEADS_PER_RUN:
            break

        restaurant_name = lead['Restaurant Name']
        target_url = lead['Website URL']
        
        try:
            cell = leads_worksheet.find(restaurant_name)
            target_row_number = cell.row
        except gspread.exceptions.CellNotFound:
            print(f"Could not find row for {restaurant_name}, skipping.")
            continue
            
        print(f"--- Processing Lead: {restaurant_name} ---")

        # --- Part 2: Scrape the Website ---
        body_html = ""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(target_url, timeout=60000)
                body_html = page.locator('body').inner_html()
            except Exception as e:
                print(f"Error scraping {target_url}: {e}")
                leads_worksheet.update_cell(target_row_number, 6, "Processing Error - Scraping Failed")
                continue
            finally:
                browser.close()

        # --- Part 3: The AI "Chain" ---
        try:
            prompt1 = f"""
            Analyze the raw HTML of {target_url}. Perform two tasks:
            TASK 1: Extract Data (About Us, Phone, Email, Social Media).
            TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
            HTML: {body_html}
            """
            response1 = model.generate_content(prompt1)
            flaw_analysis = response1.text
            
            prompt2 = f"Based on the following website analysis, generate a detailed prompt for an AI website builder. ANALYSIS: {flaw_analysis}"
            response2 = model.generate_content(prompt2)
            builder_prompt = response2.text

            # --- Part 4: Log to Sheets ---
            results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt, "", ""]) # Added empty cells for new columns
            
            leads_worksheet.update_cell(target_row_number, 6, "Analysis Complete")
            processed_count += 1
            print(f"✅ Successfully processed {restaurant_name}")
        except Exception as e:
            print(f"Error during AI chain or sheet update for {restaurant_name}: {e}")
            leads_worksheet.update_cell(target_row_number, 6, "Processing Error - AI Failed")
            continue

    return f"Processor run complete! Processed {processed_count} leads."

if __name__ == '__main__':
    # We changed the port to 10001 earlier
    app.run(host='0.0.0.0', port=10001)
