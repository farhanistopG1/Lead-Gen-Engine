from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import time

# ---------------- CONFIG ----------------
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY_HERE"
SPREADSHEET_NAME = "Lead Gen Engine"
SHEET_UPDATE_DELAY = 1  # seconds between sheet updates to avoid rate limits

# ---------------- CONNECT TO SHEETS ----------------
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    leads_worksheet = spreadsheet.worksheet("LEADS")
    results_worksheet = spreadsheet.worksheet("RESULTS")
except Exception as e:
    print(f"Error connecting to Google Sheets: {e}")
    exit(1)

# ---------------- CONFIGURE GEMINI ----------------
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")
except Exception as e:
    print(f"Error configuring Gemini client: {e}")
    exit(1)

# ---------------- GET FIRST PENDING LEAD ----------------
all_leads = leads_worksheet.get_all_records()
lead_to_process = None
lead_row_index = None

# Find the first pending lead
for idx, lead in enumerate(all_leads):
    if str(lead.get("Status", "")).strip().lower() == "pending":
        lead_to_process = lead
        lead_row_index = idx + 2  # +2 because enumerate starts at 0 and sheet rows start at 1, plus header row
        break

if not lead_to_process:
    print("No pending leads found.")
    exit(0)

restaurant_name = lead_to_process["Restaurant Name"]
target_url = lead_to_process.get("Website URL", "").strip()

print(f"--- Processing Lead: {restaurant_name} (Row {lead_row_index}) ---")

# Check if URL is valid
if not target_url or target_url.lower() in ["no website found", ""]:
    print(f"❌ Invalid URL for {restaurant_name}")
    leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - No Valid URL")
    exit(1)

# ---- MARK LEAD AS PROCESSING ----
try:
    leads_worksheet.update_cell(lead_row_index, 6, "Processing...")
    time.sleep(SHEET_UPDATE_DELAY)
    print(f"🔄 Marked {restaurant_name} as Processing...")
except Exception as e:
    print(f"Error updating status to Processing: {e}")

# ---- SCRAPE WEBSITE ----
body_html = ""
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            print(f"🌐 Scraping website: {target_url}")
            page.goto(target_url, timeout=60000)
            body_html = page.locator("body").inner_html()
            print(f"✅ Successfully scraped {len(body_html)} characters from {target_url}")
        except Exception as e:
            print(f"❌ Error scraping {target_url}: {e}")
            leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - Scraping Failed")
            exit(1)
        finally:
            browser.close()
except Exception as e:
    print(f"❌ Playwright error for {target_url}: {e}")
    leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - Scraping Failed")
    exit(1)

# ---- AI ANALYSIS ----
try:
    print(f"🤖 Starting AI analysis for {restaurant_name}")
    
    # First AI prompt - Extract data and analyze flaws
    prompt1 = f"""
    Analyze the raw HTML of {target_url}. Perform two tasks:
    TASK 1: Extract Data (About Us, Phone, Email, Social Media).
    TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
    HTML: {body_html}
    """
    response1 = model.generate_content(prompt1)
    flaw_analysis = response1.text
    print(f"✅ Completed flaw analysis for {restaurant_name}")
    
    # Second AI prompt - Generate builder prompt
    prompt2 = f"""Based on the following website analysis, generate a detailed prompt for an AI website builder.
ANALYSIS: {flaw_analysis}"""
    response2 = model.generate_content(prompt2)
    builder_prompt = response2.text
    print(f"✅ Generated builder prompt for {restaurant_name}")

    # ---- LOG RESULTS TO SHEETS ----
    print(f"📊 Logging results to spreadsheet...")
    results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt, "", ""])
    time.sleep(SHEET_UPDATE_DELAY)
    
    # ---- MARK LEAD AS COMPLETE ----
    leads_worksheet.update_cell(lead_row_index, 6, "Complete")
    time.sleep(SHEET_UPDATE_DELAY)
    
    print(f"✅ Successfully processed and completed {restaurant_name}")
    print(f"🎉 Lead processing finished! Status updated to 'Complete'")

except Exception as e:
    print(f"❌ Error during AI analysis or sheet update for {restaurant_name}: {e}")
    leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - AI Failed")
    exit(1)

print(f"\n🏁 Script completed successfully!")
print(f"📋 Processed: {restaurant_name}")
print(f"📍 Row: {lead_row_index}")
print(f"✅ Status: Complete")
