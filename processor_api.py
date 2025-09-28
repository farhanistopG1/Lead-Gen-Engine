import google.generativeai as genai
import gspread
from playwright.sync_api import sync_playwright
import os
import time

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"  # replace with your key
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3
SHEET_UPDATE_DELAY = 1  # delay between sheet updates to avoid quota errors

# --- CONFIGURE AI ---
genai.configure(api_key=GEMINI_API_KEY)

# --- GOOGLE SHEETS SETUP ---
creds_path = '/root/Lead-Gen-Engine/gspread_credentials.json'  # replace with your path
gc = gspread.service_account(filename=creds_path)
spreadsheet = gc.open(SPREADSHEET_NAME)
leads_worksheet = spreadsheet.worksheet("LEADS")
results_worksheet = spreadsheet.worksheet("RESULTS")


def process_leads():
    all_leads = leads_worksheet.get_all_records()
    leads_to_process = [lead for lead in all_leads if str(lead.get('Status')).strip() == 'Pending']

    if not leads_to_process:
        print("No pending leads found.")
        return

    processed_count = 0

    for idx, lead in enumerate(leads_to_process, start=2):  # Google Sheets row index starts at 1; header row is 1
        if processed_count >= MAX_LEADS_PER_RUN:
            break

        restaurant_name = lead.get('Restaurant Name', 'Unknown')
        target_url = lead.get('Website URL', '')

        if not target_url.startswith("http"):
            print(f"Skipping invalid URL for {restaurant_name}: {target_url}")
            leads_worksheet.update_cell(idx, 6, "Processing Error - Invalid URL")
            time.sleep(SHEET_UPDATE_DELAY)
            continue

        print(f"--- Processing Lead: {restaurant_name} ---")

        # --- SCRAPE WEBSITE ---
        body_html = ""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(target_url, timeout=60000)
                body_html = page.locator('body').inner_html()
            except Exception as e:
                print(f"Error scraping {target_url}: {e}")
                leads_worksheet.update_cell(idx, 6, "Processing Error - Scraping Failed")
                time.sleep(SHEET_UPDATE_DELAY)
                continue
            finally:
                browser.close()

        # --- AI ANALYSIS ---
        try:
            prompt = f"""
            Analyze the raw HTML of {target_url}. Perform two tasks:
            1. Extract Data (About Us, Phone, Email, Social Media).
            2. Provide a Strategic Analysis of 3 critical website flaws.
            HTML: {body_html}
            """

            response = genai.generate(
                model="gemini-2.5-flash",
                prompt=prompt,
                temperature=0.7,
                max_output_tokens=800
            )

            flaw_analysis = response.text

            # --- UPDATE SHEETS ---
            results_worksheet.append_row([restaurant_name, flaw_analysis, "", "", ""])
            leads_worksheet.update_cell(idx, 6, "Analysis Complete")
            proce

