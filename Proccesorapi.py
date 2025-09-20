# === Step 1: Define the entire Python script as a text string ===

script_code = """
import sys
from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import time
import re
import json

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
KEY_FILE_NAME = "new-mac-project-5bf7c5fa874a.json"
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3

def process_lead(lead, leads_worksheet, results_worksheet, model):
    # A function to process a single lead, making it reusable for retries.
    restaurant_name = lead['Restaurant Name']
    target_url = lead['Website URL']
    target_row_number = lead['row_number']
    
    print(f"\\n--- Processing Lead: {restaurant_name} ---")
    
    try:
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
                return False # Indicate failure
            finally:
                browser.close()

        # --- AI Agent Analyzes the Blueprint ---
        print("--- SENDING BLUEPRINT TO GEMINI AI AGENT FOR ANALYSIS ---")
        prompt = f\"\"\"
        Analyze the raw HTML of {target_url}. Perform two tasks:
        TASK 1: Extract Data (About Us, Phone, Email, Social Media).
        TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
        HTML: {body_html}
        \"\"\"
        response = model.generate_content(prompt)
        flaw_analysis = response.text
        print("✅ Flaw analysis complete.")
        
        print("--- Sending Analysis to AI Prompt Engineer ---")
        prompt2 = f"Based on the following website analysis, generate a detailed, high-quality prompt for an AI website builder. ANALYSIS: {flaw_analysis}"
        response2 = model.generate_content(prompt2)
        builder_prompt = response2.text
        print("✅ Builder prompt generated.")

        # --- Log to RESULTS Sheet & Update LEADS Status ---
        print("--- Logging analysis to 'RESULTS' sheet... ---")
        if not results_worksheet.get_all_values():
             results_worksheet.append_row(["Restaurant Name", "Flaw Analysis", "Builder Prompt"])
        results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt])
        
        print("--- Updating status in 'LEADS' sheet... ---")
        leads_worksheet.update_cell(target_row_number, 6, "Analysis Complete")
        print("✅ Sheets successfully updated!")
        return True # Indicate success

    except Exception as e:
        print(f"❌ An error occurred during processing: {e}")
        leads_worksheet.update_cell(target_row_number, 6, "Processing Error - AI Failed")
        return False # Indicate failure

def main():
    # --- Part 1: Connect to Google Sheets & Find Tasks ---
    try:
        gc = gspread.service_account(filename=KEY_FILE_NAME)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
        print("✅ Successfully connected to Google Sheets.")
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return

    all_leads = leads_worksheet.get_all_records()
    leads_to_process = []
    for i, lead in enumerate(all_leads):
        if str(lead.get('Status', '')).strip() == 'Pending' and str(lead.get('Website URL', '')).startswith('http'):
            lead['row_number'] = i + 2
            leads_to_process.append(lead)
            if len(leads_to_process) >= MAX_LEADS_PER_RUN:
                break
    
    if not leads_to_process:
        print("✅ No pending leads with websites found to process.")
        return
        
    print(f"Found {len(leads_to_process)} leads to process.")
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    failed_leads = []

    for lead in leads_to_process:
        success = process_lead(lead, leads_worksheet, results_worksheet, model)
        if not success:
            failed_leads.append(lead)
    
    # --- FINAL RETRY LOGIC ---
    if failed_leads:
        print("\\n--- RETRYING FAILED LEADS (1 attempt) ---")
        for lead in failed_leads:
            print(f"Retrying lead: {lead['Restaurant Name']}")
            process_lead(lead, leads_worksheet, results_worksheet, model)

    print("\\n🎉 Full batch and retry process complete!")

main()
"""

# === Step 2: Write the script to a file ===
with open("ultimate_processor_v2.py", "w") as file:
    file.write(script_code)
print("✅ Script file 'ultimate_processor_v2.py' created successfully.")

# === Step 3: Execute the script ===
print("\n--- EXECUTING SCRIPT ---\n")
!python3 ultimate_processor_v2.py