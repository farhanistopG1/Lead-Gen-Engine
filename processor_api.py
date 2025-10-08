from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import time
import random
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re

# ---------------- CONFIG ----------------
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"
SHEET_UPDATE_DELAY = 3
MAX_LEADS_PER_DAY = 100

# 🔥🔥🔥 CRITICAL: TIMING IN SECONDS NOT MINUTES! 🔥🔥🔥
MIN_DELAY_SECONDS = 30   # 30-90 SECONDS between leads
MAX_DELAY_SECONDS = 90   # NOT minutes!
RETRY_DELAY_SECONDS = 120  # 2 minutes on failure

TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
CACHE_DURATION = 300
MAX_RETRIES = 5
BASE_BACKOFF = 10
MAX_HTML_LENGTH = 8000
MIN_HTML_LENGTH = 500

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
                print(f"📦 Using cached data for {key}")
                return cached_data['data']
        return None
    
    def set(self, key, data):
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
        self.save_cache()

cache = SheetsCache()

# ---------------- HTML CLEANING ----------------
def clean_html_aggressive(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'path', 
                        'meta', 'link', 'head', 'footer', 'nav', 'aside']):
            tag.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'(\S)\1{3,}', r'\1\1', text)
        text = re.sub(r'[^\w\s@.,!?;:()\-\'\"\/]', '', text)
        
        structured_data = {
            'title': soup.title.string if soup.title else '',
            'headings': [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2', 'h3'])[:8]],
            'meta_desc': '',
            'contact_info': extract_contact_info(text),
        }
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            structured_data['meta_desc'] = meta_desc['content'][:150]
        
        if len(text) > MAX_HTML_LENGTH:
            mid_point = MAX_HTML_LENGTH // 2
            text = text[:mid_point] + " [...] " + text[-mid_point:]
        
        compact_html = f"""
TITLE: {structured_data['title']}
DESC: {structured_data['meta_desc']}
HEADINGS: {', '.join(structured_data['headings'])}
CONTACT: {json.dumps(structured_data['contact_info'])}

TEXT:
{text}
"""
        return compact_html.strip()
        
    except Exception as e:
        print(f"⚠️  HTML cleaning error: {e}")
        return html_content[:MAX_HTML_LENGTH]

def extract_contact_info(text):
    contact = {}
    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    if emails:
        contact['emails'] = list(set(emails))[:3]
    phones = re.findall(r'[\+\(]?[0-9][0-9\s\-\(\)]{8,}[0-9]', text)
    if phones:
        contact['phones'] = list(set([p.strip() for p in phones]))[:3]
    if 'instagram' in text.lower() or '@' in text:
        contact['has_social'] = True
    return contact

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
                print(f"⚠️  Rate limit hit during {operation_name}. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"❌ API Error during {operation_name}: {e}")
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            print(f"❌ Error during {operation_name}: {e}")
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
                print(f"⚠️  Rate limit hit during {operation_name}. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"❌ API Error during {operation_name}: {e}")
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            print(f"❌ Error during {operation_name}: {e}")
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
    print("✅ Connected to Google Sheets")
except Exception as e:
    print(f"❌ FATAL: Error connecting to Google Sheets: {e}")
    exit(1)

# ---------------- CONFIGURE GEMINI ----------------
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    print("✅ Configured Gemini AI with Flash-Lite")
except Exception as e:
    print(f"❌ FATAL: Error configuring Gemini: {e}")
    exit(1)

# ---------------- NORMALIZATION ----------------
def normalize_text(text):
    if not text:
        return ""
    import re
    cleaned = re.sub(r'[^a-z0-9\s]', '', str(text).lower())
    return ' '.join(cleaned.split())

def normalize_phone(phone):
    if not phone:
        return ""
    digits = ''.join(filter(str.isdigit, str(phone)))
    return digits[-10:] if len(digits) >= 10 else digits

# ---------------- UNIFIED DUPLICATE KEY ----------------
def create_duplicate_key(name, phone):
    name_norm = normalize_text(name)
    phone_norm = normalize_phone(phone)
    
    if phone_norm and len(phone_norm) == 10:
        return f"phone:{phone_norm}"
    
    if name_norm:
        return f"name:{name_norm}"
    
    return None

# ---------------- PHONE NUMBER SYNC ----------------
def sync_phone_numbers_from_leads():
    print("\n🔄 === SYNCING PHONE NUMBERS ===")
    
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Reading RESULTS for sync",
            None
        )
        
        if not results_data:
            print("ℹ️  No data in RESULTS")
            return
        
        leads_data = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Reading LEADS for sync",
            None
        )
        
        leads_lookup = {}
        for lead in leads_data:
            name_norm = normalize_text(lead.get("Restaurant Name", ""))
            phone = str(lead.get("Phone Number", "")).strip()
            if name_norm:
                leads_lookup[name_norm] = phone if phone else "No Number"
        
        updates_made = 0
        for idx, result_row in enumerate(results_data):
            row_num = idx + 2
            restaurant_name = str(result_row.get("Restaurant Name", "")).strip()
            current_phone = str(result_row.get("Phone Number", "")).strip()
            
            name_norm = normalize_text(restaurant_name)
            correct_phone = leads_lookup.get(name_norm, "No Number")
            
            if not current_phone or current_phone != correct_phone:
                print(f"  📝 Row {row_num}: '{restaurant_name}' → {correct_phone}")
                try:
                    safe_sheet_write(
                        lambda: results_worksheet.update_cell(row_num, 6, correct_phone),
                        f"Syncing phone to F{row_num}"
                    )
                    updates_made += 1
                except Exception as e:
                    print(f"  ❌ Failed row {row_num}: {e}")
        
        print(f"✅ Synced {updates_made} phones" if updates_made else "✅ All phones in sync")
        print("=== SYNC COMPLETE ===\n")
        
    except Exception as e:
        print(f"❌ Sync error: {e}")

# ---------------- CLEANUP WITH UNIFIED LOGIC ----------------
def clean_duplicates_in_results():
    print("\n🧹 === CLEANING DUPLICATES ===")
    
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Reading RESULTS for cleanup",
            None
        )
        
        if not results_data:
            print("ℹ️  No data in RESULTS")
            return
        
        seen = {}
        rows_to_delete = []
        
        for idx, row in enumerate(results_data):
            row_num = idx + 2
            name = str(row.get("Restaurant Name", "")).strip()
            phone = str(row.get("Phone Number", "")).strip()
            
            dup_key = create_duplicate_key(name, phone)
            
            if not dup_key:
                print(f"  ⚠️  Row {row_num}: No valid name or phone - skipping")
                continue
            
            if dup_key in seen:
                print(f"  🗑️  DUPLICATE: {name} (Row {row_num}) matches Row {seen[dup_key]}")
                print(f"     Key: {dup_key}")
                rows_to_delete.append(row_num)
            else:
                seen[dup_key] = row_num
        
        if rows_to_delete:
            print(f"\n🗑️  Deleting {len(rows_to_delete)} duplicates...")
            for row_num in sorted(rows_to_delete, reverse=True):
                try:
                    safe_sheet_write(
                        lambda r=row_num: results_worksheet.delete_rows(r),
                        f"Deleting row {row_num}"
                    )
                    print(f"  ✅ Deleted row {row_num}")
                except Exception as e:
                    print(f"  ❌ Failed to delete row {row_num}: {e}")
        else:
            print("✅ No duplicates found")
        
        print("=== CLEANUP COMPLETE ===\n")
        
    except Exception as e:
        print(f"❌ Cleanup error: {e}")

# ---------------- DUPLICATE CHECK ----------------
def is_already_processed(restaurant_name, phone_raw):
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Checking duplicates",
            None
        )
        
        new_key = create_duplicate_key(restaurant_name, phone_raw)
        
        if not new_key:
            print(f"  ⚠️  WARNING: No valid name or phone for duplicate check")
            return False
        
        for row in results_data:
            existing_name = str(row.get("Restaurant Name", "")).strip()
            existing_phone = str(row.get("Phone Number", "")).strip()
            
            existing_key = create_duplicate_key(existing_name, existing_phone)
            
            if existing_key and new_key == existing_key:
                print(f"  ❌ DUPLICATE FOUND")
                print(f"     New: {restaurant_name} | {phone_raw}")
                print(f"     Existing: {existing_name} | {existing_phone}")
                print(f"     Match key: {new_key}")
                return True
        
        print(f"  ✅ NEW (Key: {new_key})")
        return False
        
    except Exception as e:
        print(f"❌ Duplicate check error: {e}")
        return False

# ---------------- PROCESSING ----------------
def process_single_lead():
    try:
        all_leads = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Fetching LEADS",
            "leads_all"
        )
    except Exception as e:
        print(f"❌ Failed to fetch leads: {e}")
        return False
    
    for idx, lead in enumerate(all_leads):
        status = str(lead.get("Status", "")).strip().lower()
        restaurant_name = str(lead.get("Restaurant Name", "")).strip()
        phone_raw = str(lead.get("Phone Number", "")).strip()
        
        if status == "pending":
            lead_row_index = idx + 2
            
            print(f"\n{'='*60}")
            print(f"🔍 Checking: {restaurant_name} (Row {lead_row_index})")
            print(f"{'='*60}")
            
            if is_already_processed(restaurant_name, phone_raw):
                print(f"⏭️  SKIP: Already in RESULTS")
                try:
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                except:
                    pass
                continue
            
            target_url = lead.get("Website URL", "").strip()
            
            print(f"🚀 Processing: {restaurant_name}")
            
            if not target_url or target_url.lower() in ["no website found", ""]:
                print(f"❌ Invalid URL")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - No Valid URL"),
                    "Updating status"
                )
                return False
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, f"Processing... {timestamp}"),
                "Marking as processing"
            )
            
            # SCRAPE AND CLEAN
            try:
                print(f"🌐 Scraping: {target_url}")
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    try:
                        page.goto(target_url, timeout=60000)
                        body_html = page.locator("body").inner_html()
                        print(f"   📄 Raw HTML: {len(body_html):,} chars")
                        
                        cleaned_html = clean_html_aggressive(body_html)
                        reduction_pct = 100 - int(len(cleaned_html)/len(body_html)*100)
                        print(f"   ✨ Cleaned HTML: {len(cleaned_html):,} chars (reduced by {reduction_pct}%)")
                        
                    finally:
                        browser.close()
            except Exception as e:
                print(f"❌ Scraping failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - Scraping Failed"),
                    "Updating status"
                )
                return False
            
            # AI ANALYSIS
            try:
                print(f"🤖 Starting AI analysis (ULTRA-minimal)...")
                
                minimal_prompt = f"""Analyze "{restaurant_name}" website and provide:

1. KEY INFO (3-4 bullets: what they do, contact found/missing, main issues)
2. FIX CHECKLIST (5-7 items: missing email, broken links, what to add)

Keep it SHORT and actionable.

WEBSITE DATA:
{cleaned_html}"""
                
                start_time = time.time()
                response = model.generate_content(minimal_prompt)
                full_response = response.text
                api_time = time.time() - start_time
                
                # COST TRACKING
                input_tokens = int((len(cleaned_html) + len(minimal_prompt)) / 4)
                output_tokens = int(len(full_response) / 4)
                total_tokens = input_tokens + output_tokens
                
                input_cost_usd = (input_tokens / 1_000_000) * 0.10
                output_cost_usd = (output_tokens / 1_000_000) * 0.40
                total_cost_inr = (input_cost_usd + output_cost_usd) * 85
                
                print(f"   ✅ AI completed in {api_time:.1f}s")
                print(f"   📊 Tokens: {input_tokens:,} in + {output_tokens:,} out = {total_tokens:,}")
                print(f"   💰 Cost: ₹{total_cost_inr:.4f} (~₹{total_cost_inr:.2f})")
                
                flaw_analysis = full_response
                builder_prompt = f"Template-based fixes (see analysis)"
                
                print("🔍 Final duplicate check...")
                if is_already_processed(restaurant_name, phone_raw):
                    print(f"❌ DUPLICATE in final check - aborting")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                    return False
                
                phone_to_save = phone_raw if phone_raw else "No Number"
                
                print(f"💾 Saving to RESULTS...")
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,
                        flaw_analysis,
                        builder_prompt,
                        "",
                        "",
                        phone_to_save
                    ]),
                    "Appending to RESULTS"
                )
                
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking complete"
                )
                
                print(f"✅ Successfully processed: {restaurant_name}")
                print(f"{'='*60}\n")
                return True
                
            except Exception as e:
                print(f"❌ AI analysis failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - AI Failed"),
                    "Updating status"
                )
                return False
    
    print("ℹ️  No pending leads found")
    return False

# ---------------- MAIN LOOP ----------------
print("\n" + "="*70)
print("🚀 ULTRA-OPTIMIZED Lead Processor v3.0")
print("="*70)
print(f"💎 Model: Gemini 2.5 Flash-Lite")
print(f"🧹 HTML Cleaning: 90%+ reduction")
print(f"⚡ API Calls: 1 per lead")
print(f"📊 Daily Limit: {MAX_LEADS_PER_DAY} leads")
print(f"⏱️  Delay: {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS} SECONDS (not minutes!)")
print(f"💰 Est. Cost: ₹{(MAX_LEADS_PER_DAY * 30 * 0.01):.2f}/month")
print("="*70 + "\n")

clean_duplicates_in_results()
sync_phone_numbers_from_leads()

processing_times = []

while True:
    try:
        daily_log = load_daily_log()
        daily_log = reset_daily_count_if_new_day(daily_log)
        
        if daily_log["processed_count"] >= MAX_LEADS_PER_DAY:
            print(f"🎯 Daily limit reached ({MAX_LEADS_PER_DAY} leads)")
            now = datetime.now()
            tomorrow = now.replace(hour=0, minute=1, second=0, microsecond=0) + timedelta(days=1)
            sleep_seconds = (tomorrow - now).total_seconds()
            print(f"😴 Sleeping until {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(sleep_seconds)
            continue
        
        lead_start_time = time.time()
        success = process_single_lead()
        lead_end_time = time.time()
        
        if success:
            processing_time = lead_end_time - lead_start_time
            processing_times.append(processing_time)
            
            daily_log["processed_count"] += 1
            daily_log["last_processed"] = datetime.now().isoformat()
            save_daily_log(daily_log)
            
            remaining = MAX_LEADS_PER_DAY - daily_log["processed_count"]
            avg_time = sum(processing_times) / len(processing_times)
            
            print(f"\n📈 PROGRESS")
            print(f"   ✅ Completed: {daily_log['processed_count']}/{MAX_LEADS_PER_DAY}")
            print(f"   ⏳ Remaining: {remaining}")
            print(f"   ⏱️  Avg time: {avg_time:.1f}s per lead")
            print(f"   💰 Cost today: ₹{(daily_log['processed_count'] * 0.01):.2f}")
            
            if remaining > 0:
                # 🔥🔥🔥 CRITICAL: DELAYS IN SECONDS! 🔥🔥🔥
                delay_seconds = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                print(f"   ⏸️  Waiting {delay_seconds} SECONDS...")
                print(f"   📅 ETA: ~{int(remaining * (avg_time + delay_seconds) / 60)} minutes\n")
                time.sleep(delay_seconds)  # THIS MUST BE SECONDS!
        else:
            print(f"⚠️  No leads. Waiting {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)  # THIS MUST BE SECONDS!
            
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user")
        print(f"📊 Processed {daily_log['processed_count']} leads today")
        print(f"💰 Cost: ₹{(daily_log['processed_count'] * 0.01):.2f}")
        break
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"⏸️  Waiting {RETRY_DELAY_SECONDS} seconds...")
        time.sleep(RETRY_DELAY_SECONDS)

print("\n✋ Processor stopped")
