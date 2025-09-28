from google import genai
import gspread
from playwright.sync_api import sync_playwright
import os
import time

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3
SHEET_UPDATE_DELAY = 1  # Delay in seconds between sheet updates to avoid quota issues

# Initialize the Gemini client
genai.configure(api_key=GEMINI_API_KEY)
client = genai.Client()

# Authenticate with Google Sheets
gc = gspread.service_account(filename='/root/Lead-Gen-Engine/gspread_credentials.json')
spreadsheet = gc.open(SPREADSHEET_NAME)
leads_worksheet = spreadsheet.worksheet("LEADS")
results_worksheet = spreadsheet.worksheet("RESULTS")

# --- MAIN PROCESSING FUNCTION ---
def process_leads():
    all_leads = leads_worksheet.get_all_records()
    leads_to_process = [lead for lead in all_leads if str(lead.get('Status')).strip() == 'Pending']
    
    if not leads_to_process:
        print("No pending leads found.")
        return

    processed_count = 0
    for lead in leads_to_process:
        if processed_count >= MAX_LEADS_PER_RUN:
            break

        restaurant_name = lead['Restaurant Name']
        target_url = lead['Website URL']
        
        if not target_url.startswith("http"):
            print(f"Skipping invalid URL for {restaurant_name}: {target_url}")
            leads_worksheet.update_cell(lead.row, 6, "Processing Error - Invalid URL")
            time.sleep(SHEET_UPDATE_DELAY)
            continue

        print(f"--- Processing Lead: {restaurant_name} ---")

        # --- Scrape the Website ---
        body_html = ""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(target_url, timeout=60000)
                body_html = page.locator('body').inner_html()
            except Exception as e:
                print(f"Error scraping {target_url}: {e}")
                leads_worksheet.update_cell(lead.row, 6, "Processing Error - Scraping Failed")
                time.sleep(SHEET_UPDATE_DELAY)
                continue
            finally:
                browser.close()

        # --- AI Analysis ---
        try:
            prompt = f"""
            Analyze the raw HTML of {target_url}. Perform two tasks:
            TASK 1: Extract Data (About Us, Phone, Email, Social Media).
            TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
            HTML: {body_html}
            """
            response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
            flaw_analysis = response.text

            # --- Log to Sheets ---
            results_worksheet.append_row([restaurant_name, flaw_analysis, "", "", ""])
            leads_worksheet.update_cell(lead.row, 6, "Analysis Complete")
            processed_count += 1
            print(f"✅ Successfully processed {restaurant_name}")
        except Exception as e:
            print(f"Error during AI analysis for {restaurant_name}: {e}")
            leads_worksheet.update_cell(lead.row, 6, "Processing Error - AI Failed")
            time.sleep(SHEET_UPDATE_DELAY)
            continue

        time.sleep(SHEET_UPDATE_DELAY)

    print(f"Processing complete! Processed {processed_count} leads.")

if __name__ == '__main__':
    process_leads()
