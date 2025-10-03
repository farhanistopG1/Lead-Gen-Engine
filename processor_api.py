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
SHEET_UPDATE_DELAY = 5
MAX_LEADS_PER_DAY = 3
MIN_DELAY_MINUTES = 10
MAX_DELAY_MINUTES = 20
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
CACHE_DURATION = 300
MAX_RETRIES = 5
BASE_BACKOFF = 10

# ---------------- CACHING LAYER ----------------
class SheetsCache:
    def __init__(self):
        self.cache = self.load_cache()
    
    def load_cache(self):
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_cache(self):
        with open(CACHE_FILE, 'w') as f:
            json.dump(self.cache, f)
    
    def get(self, key):
        if key in self.cache:
            cached_data = self.cache[key]
            if time.time() - cached_data['timestamp'] < CACHE_DURATION:
                print(f"Using cached data for {key}")
                return cached_data['data']
        return None
    
    def set(self, key, data):
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
        self.save_cache()

cache = SheetsCache()

# ---------------- RATE LIMIT SAFE OPERATIONS ----------------
def safe_sheet_read(operation, operation_name, cache_key=None, max_retries=MAX_RETRIES):
    if cache_key:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
    
    for attempt in range(max_retries):
        try:
            result = operation()
            if cache_key:
                cache.set(cache_key, result)
            time.sleep(2)
            return result
        except gspread.exceptions.APIError as e:
            if '429' in str(e):
                wait_time = (2 ** attempt) * BASE_BACKOFF
                print(f"Rate limit hit during {operation_name}. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"API Error during {operation_name}: {e}")
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            print(f"Error during {operation_name}: {e}")
            time.sleep(BASE_BACKOFF)
    
    raise Exception(f"Failed {operation_name} after {max_retries} attempts")

def safe_sheet_write(operation, operation_name, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = operation()
            time.sleep(SHEET_UPDATE_DELAY)
            cache.cache = {}
            cache.save_cache()
            return result
        except gspread.exceptions.APIError as e:
            if '429' in str(e):
                wait_time = (2 ** attempt) * BASE_BACKOFF
                print(f"Rate limit hit during {operation_name}. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"API Error during {operation_name}: {e}")
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            print(f"Error during {operation_name}: {e}")
            time.sleep(BASE_BACKOFF)
    
    raise Exception(f"Failed {operation_name} after {max_retries} attempts")

# ---------------- DAILY TRACKING ----------------
def load_daily_log():
    if os.path.exists(TRACKING_FILE):
        try:
            with open(TRACKING_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"date": "", "processed_count": 0, "last_processed": ""}
    return {"date": "", "processed_count": 0, "last_processed": ""}

def save_daily_log(data):
    with open(TRACKING_FILE, 'w') as f:
        json.dump(data, f)

def reset_daily_count_if_new_day(log_data):
    today = datetime.now().strftime("%Y-%m-%d")
    if log_data["date"] != today:
        log_data["date"] = today
        log_data["processed_count"] = 0
        log_data["last_processed"] = ""
        save_daily_log(log_data)
    return log_data

# ---------------- CONNECT TO SHEETS ----------------
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    leads_worksheet = spreadsheet.worksheet("LEADS")
    results_worksheet = spreadsheet.worksheet("RESULTS")
    print("Connected to Google Sheets")
except Exception as e:
    print(f"FATAL: Error connecting to Google Sheets: {e}")
    exit(1)

# ---------------- CONFIGURE GEMINI ----------------
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")
    print("Configured Gemini AI")
except Exception as e:
    print(f"FATAL: Error configuring Gemini: {e}")
    exit(1)

# ---------------- PHONE NUMBER UTILITIES ----------------
def normalize_phone(phone):
    """Normalize phone number for comparison"""
    if not phone:
        return ""
    return ''.join(filter(str.isdigit, str(phone)))[-10:]

def get_phone_from_leads(restaurant_name):
    """Fetch phone number from LEADS sheet by restaurant name"""
    try:
        leads_data = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Reading LEADS for phone lookup",
            "leads_phone_lookup"
        )
        
        name_lower = restaurant_name.strip().lower()
        for lead in leads_data:
            lead_name = str(lead.get("Restaurant Name", "")).strip().lower()
            if lead_name == name_lower:
                phone = str(lead.get("Phone Number", "")).strip()
                return phone if phone else "No Number"
        
        return "No Number"
    except Exception as e:
        print(f"Error fetching phone from LEADS: {e}")
        return "No Number"

# ---------------- PHONE NUMBER SYNC ----------------
def sync_phone_numbers_from_leads():
    """Sync all phone numbers from LEADS to RESULTS"""
    print("\n=== SYNCING PHONE NUMBERS FROM LEADS ===")
    
    try:
        # Get RESULTS data
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Reading RESULTS for phone sync",
            "results_phone_sync"
        )
        
        if not results_data:
            print("No data in RESULTS sheet")
            return
        
        # Get LEADS data
        leads_data = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Reading LEADS for phone sync",
            "leads_phone_sync"
        )
        
        # Build lookup: restaurant name -> phone number
        leads_lookup = {}
        for lead in leads_data:
            name = str(lead.get("Restaurant Name", "")).strip().lower()
            phone = str(lead.get("Phone Number", "")).strip()
            leads_lookup[name] = phone if phone else "No Number"
        
        # Update RESULTS where phone is missing or incorrect
        updates_made = 0
        for idx, result_row in enumerate(results_data):
            row_num = idx + 2  # Account for header
            restaurant_name = str(result_row.get("Restaurant Name", "")).strip()
            current_phone = str(result_row.get("Phone Number", "")).strip()
            
            name_lower = restaurant_name.lower()
            correct_phone = leads_lookup.get(name_lower, "No Number")
            
            # Update if phone is missing or different
            if not current_phone or current_phone != correct_phone:
                print(f"Updating phone for '{restaurant_name}' (Row {row_num}): {correct_phone}")
                try:
                    safe_sheet_write(
                        lambda: results_worksheet.update_cell(row_num, 6, correct_phone),
                        f"Syncing phone to F{row_num}"
                    )
                    updates_made += 1
                except Exception as e:
                    print(f"Failed to update phone for row {row_num}: {e}")
        
        if updates_made > 0:
            print(f"\nSynced {updates_made} phone numbers from LEADS to RESULTS")
        else:
            print("All phone numbers already in sync")
        
        print("=== PHONE SYNC COMPLETE ===\n")
        
    except Exception as e:
        print(f"Error during phone sync: {e}")

# ---------------- DUPLICATE CLEANUP ----------------
def clean_duplicates_in_results():
    """Remove duplicate entries from RESULTS sheet"""
    print("\n=== STARTING DUPLICATE CLEANUP ===")
    
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Reading RESULTS for cleanup",
            "results_cleanup"
        )
        
        if not results_data:
            print("No data in RESULTS sheet")
            return
        
        seen = {}
        rows_to_delete = []
        
        for idx, row in enumerate(results_data):
            row_num = idx + 2
            name = str(row.get("Restaurant Name", "")).strip().lower()
            phone = normalize_phone(row.get("Phone Number", ""))
            
            key = f"{name}|{phone}"
            
            if key in seen:
                print(f"DUPLICATE: {row.get('Restaurant Name')} (Row {row_num}) - Same as Row {seen[key]}")
                rows_to_delete.append(row_num)
            else:
                seen[key] = row_num
        
        if rows_to_delete:
            print(f"\nDeleting {len(rows_to_delete)} duplicate rows...")
            for row_num in sorted(rows_to_delete, reverse=True):
                try:
                    safe_sheet_write(
                        lambda: results_worksheet.delete_rows(row_num),
                        f"Deleting row {row_num}"
                    )
                    print(f"Deleted row {row_num}")
                except Exception as e:
                    print(f"Failed to delete row {row_num}: {e}")
            
            print(f"\nCleaned {len(rows_to_delete)} duplicates")
        else:
            print("No duplicates found")
        
        print("=== DUPLICATE CLEANUP COMPLETE ===\n")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")

def get_results_lookup():
    """Build lookup of processed leads"""
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Reading RESULTS for lookup",
            "results_lookup"
        )
        
        lookup = {}
        for row in results_data:
            name = str(row.get("Restaurant Name", "")).strip().lower()
            phone = normalize_phone(row.get("Phone Number", ""))
            key = f"{name}|{phone}"
            lookup[key] = True
        
        return lookup
    except Exception as e:
        print(f"Error building lookup: {e}")
        return {}

# ---------------- MAIN PROCESSING ----------------
def process_single_lead():
    """Process one lead"""
    
    try:
        all_leads = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Fetching LEADS",
            "leads_all"
        )
    except Exception as e:
        print(f"Failed to fetch leads: {e}")
        return False
    
    results_lookup = get_results_lookup()
    
    for idx, lead in enumerate(all_leads):
        status = str(lead.get("Status", "")).strip().lower()
        restaurant_name = str(lead.get("Restaurant Name", "")).strip()
        phone_raw = str(lead.get("Phone Number", "")).strip()
        phone_normalized = normalize_phone(phone_raw)
        
        if status == "pending":
            lookup_key = f"{restaurant_name.lower()}|{phone_normalized}"
            
            if lookup_key in results_lookup:
                print(f"SKIP: {restaurant_name} already processed")
                row_index = idx + 2
                try:
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                except:
                    pass
                continue
            
            lead_row_index = idx + 2
            target_url = lead.get("Website URL", "").strip()
            
            print(f"\n--- Processing: {restaurant_name} (Row {lead_row_index}) ---")
            
            if not target_url or target_url.lower() in ["no website found", ""]:
                print(f"Invalid URL")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - No Valid URL"),
                    "Updating invalid URL status"
                )
                return False
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, f"Processing... {timestamp}"),
                "Marking as processing"
            )
            
            # Scrape
            try:
                print(f"Scraping: {target_url}")
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    try:
                        page.goto(target_url, timeout=60000)
                        body_html = page.locator("body").inner_html()
                        print(f"Scraped {len(body_html)} characters")
                    finally:
                        browser.close()
            except Exception as e:
                print(f"Scraping failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - Scraping Failed"),
                    "Updating scraping error"
                )
                return False
            
            # AI Analysis
            try:
                print(f"Starting AI analysis...")
                
                prompt1 = f"""Analyze the raw HTML of {target_url}. Perform two tasks:
TASK 1: Extract Data (About Us, Phone, Email, Social Media).
TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
HTML: {body_html}"""
                
                response1 = model.generate_content(prompt1)
                flaw_analysis = response1.text
                print(f"Completed flaw analysis")
                
                time.sleep(3)
                
                prompt2 = f"""Based on the following website analysis, generate a detailed prompt for an AI website builder.
ANALYSIS: {flaw_analysis}"""
                
                response2 = model.generate_content(prompt2)
                builder_prompt = response2.text
                print(f"Generated builder prompt")
                
                # Final duplicate check
                results_lookup = get_results_lookup()
                if lookup_key in results_lookup:
                    print(f"DUPLICATE detected, aborting")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                    return False
                
                # Determine phone number to save
                phone_to_save = phone_raw if phone_raw else "No Number"
                
                # Save to RESULTS (6 columns: Name, Analysis, Prompt, Status, URL, Phone)
                print(f"Saving results...")
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,   # A
                        flaw_analysis,     # B
                        builder_prompt,    # C
                        "",               # D (Outreach Status)
                        "",               # E (Preview URL)
                        phone_to_save     # F (Phone Number)
                    ]),
                    "Appending to RESULTS"
                )
                
                # Mark complete
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking lead complete"
                )
                
                print(f"Successfully processed: {restaurant_name}\n")
                return True
                
            except Exception as e:
                print(f"AI analysis failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - AI Failed"),
                    "Updating AI error"
                )
                return False
    
    print("No pending leads found")
    return False

# ---------------- MAIN LOOP ----------------
print("Starting Intelligent Lead Processor")
print(f"Daily Limit: {MAX_LEADS_PER_DAY} leads")
print(f"Delay: {MIN_DELAY_MINUTES}-{MAX_DELAY_MINUTES} minutes")
print("=" * 60)

# STARTUP TASKS
clean_duplicates_in_results()
sync_phone_numbers_from_leads()

while True:
    try:
        daily_log = load_daily_log()
        daily_log = reset_daily_count_if_new_day(daily_log)
        
        if daily_log["processed_count"] >= MAX_LEADS_PER_DAY:
            print(f"Daily limit reached ({MAX_LEADS_PER_DAY} leads)")
            now = datetime.now()
            tomorrow = now.replace(hour=0, minute=1, second=0, microsecond=0) + timedelta(days=1)
            sleep_seconds = (tomorrow - now).total_seconds()
            print(f"Sleeping until {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_seconds)
            continue
        
        success = process_single_lead()
        
        if success:
            daily_log["processed_count"] += 1
            daily_log["last_processed"] = datetime.now().isoformat()
            save_daily_log(daily_log)
            
            remaining = MAX_LEADS_PER_DAY - daily_log["processed_count"]
            print(f"Progress: {daily_log['processed_count']}/{MAX_LEADS_PER_DAY}")
            print(f"Remaining: {remaining}")
            
            if remaining > 0:
                delay_minutes = random.randint(MIN_DELAY_MINUTES, MAX_DELAY_MINUTES)
                print(f"Waiting {delay_minutes} minutes...")
                time.sleep(delay_minutes * 60)
        else:
            print("Waiting 30 minutes before retry...")
            time.sleep(1800)
            
    except KeyboardInterrupt:
        print("\nStopped by user")
        break
    except Exception as e:
        print(f"Unexpected error: {e}")
        print("Waiting 10 minutes...")
        time.sleep(600)

print("\nProcessor stopped")
