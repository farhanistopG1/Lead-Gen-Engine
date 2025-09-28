from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import time
import random
import json
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"
SHEET_UPDATE_DELAY = 1  # seconds between sheet updates to avoid rate limits
MAX_LEADS_PER_DAY = 3
MIN_DELAY_MINUTES = 10
MAX_DELAY_MINUTES = 20
TRACKING_FILE = "daily_processing_log.json"

# ---------------- DAILY TRACKING FUNCTIONS ----------------
def load_daily_log():
    """Load or create daily processing log"""
    if os.path.exists(TRACKING_FILE):
        try:
            with open(TRACKING_FILE, 'r') as f:
                data = json.load(f)
            return data
        except:
            return {"date": "", "processed_count": 0, "last_processed": ""}
    return {"date": "", "processed_count": 0, "last_processed": ""}

def save_daily_log(data):
    """Save daily processing log"""
    with open(TRACKING_FILE, 'w') as f:
        json.dump(data, f)

def reset_daily_count_if_new_day(log_data):
    """Reset count if it's a new day"""
    today = datetime.now().strftime("%Y-%m-%d")
    if log_data["date"] != today:
        log_data["date"] = today
        log_data["processed_count"] = 0
        log_data["last_processed"] = ""
        save_daily_log(log_data)
    return log_data

def can_process_more_today(log_data):
    """Check if we can process more leads today"""
    return log_data["processed_count"] < MAX_LEADS_PER_DAY

def increment_daily_count(log_data, restaurant_name):
    """Increment daily processed count"""
    log_data["processed_count"] += 1
    log_data["last_processed"] = restaurant_name
    save_daily_log(log_data)

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

# ---------------- MAIN PROCESSING LOOP ----------------
def process_single_lead():
    """Process a single pending lead"""
    # Get all leads
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
        return False

    restaurant_name = lead_to_process["Restaurant Name"]
    target_url = lead_to_process.get("Website URL", "").strip()

    print(f"--- Processing Lead: {restaurant_name} (Row {lead_row_index}) ---")

    # Check if URL is valid
    if not target_url or target_url.lower() in ["no website found", ""]:
        print(f"❌ Invalid URL for {restaurant_name}")
        leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - No Valid URL")
        return False

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
                return False
            finally:
                browser.close()
    except Exception as e:
        print(f"❌ Playwright error for {target_url}: {e}")
        leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - Scraping Failed")
        return False

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
        return True

    except Exception as e:
        print(f"❌ Error during AI analysis or sheet update for {restaurant_name}: {e}")
        leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - AI Failed")
        return False

# ---------------- CONTINUOUS PROCESSING LOOP ----------------
print("🚀 Starting Auto Lead Processor...")
print(f"📋 Daily Limit: {MAX_LEADS_PER_DAY} leads")
print(f"⏰ Delay Between Leads: {MIN_DELAY_MINUTES}-{MAX_DELAY_MINUTES} minutes")
print("="*60)

while True:
    try:
        # Load and check daily log
        daily_log = load_daily_log()
        daily_log = reset_daily_count_if_new_day(daily_log)
        
        # Check if we can process more leads today
        if not can_process_more_today(daily_log):
            print(f"✋ Daily limit reached ({MAX_LEADS_PER_DAY} leads processed today)")
            print(f"💤 Sleeping until tomorrow...")
            
            # Calculate time until midnight + 1 minute
            now = datetime.now()
            tomorrow = now.replace(hour=0, minute=1, second=0, microsecond=0) + timedelta(days=1)
            sleep_seconds = (tomorrow - now).total_seconds()
            
            print(f"⏰ Will resume at {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_seconds)
            continue
        
        # Try to process a lead
        success = process_single_lead()
        
        if success:
            # Increment counter and show progress
            increment_daily_count(daily_log, "Last processed lead")
            remaining = MAX_LEADS_PER_DAY - daily_log["processed_count"]
            print(f"📈 Progress: {daily_log['processed_count']}/{MAX_LEADS_PER_DAY} leads processed today")
            print(f"📊 Remaining: {remaining} leads")
            
            if remaining > 0:
                # Random delay between leads
                delay_minutes = random.randint(MIN_DELAY_MINUTES, MAX_DELAY_MINUTES)
                delay_seconds = delay_minutes * 60
                
                print(f"⏳ Waiting {delay_minutes} minutes before next lead...")
                print(f"🕐 Next processing at: {(datetime.now() + timedelta(minutes=delay_minutes)).strftime('%H:%M:%S')}")
                print("-" * 40)
                
                time.sleep(delay_seconds)
            else:
                print(f"✅ Daily quota completed! ({MAX_LEADS_PER_DAY} leads processed)")
        else:
            # If no leads found or processing failed, wait 30 minutes before checking again
            print("⏳ No pending leads or processing failed. Waiting 30 minutes...")
            time.sleep(1800)  # 30 minutes
            
    except KeyboardInterrupt:
        print("\n⛔ Process stopped by user")
        break
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("⏳ Waiting 5 minutes before retrying...")
        time.sleep(300)  # 5 minutes

print("\n🏁 Auto Lead Processor stopped!")
