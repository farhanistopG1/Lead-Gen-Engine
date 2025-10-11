from playwright.sync_api import sync_playwright
import gspread
import os
import time
import random
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import requests

# ============================================================================
# 🔥 OLLAMA CONFIG - FREE FOREVER, UNLIMITED USAGE 🔥
# ============================================================================
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2:3b"  # Perfect for 4GB RAM + 1 CPU
SPREADSHEET_NAME = "Lead Gen Engine"
SHEET_UPDATE_DELAY = 3
MAX_LEADS_PER_DAY = 50  # Process MORE since it's FREE!
MIN_DELAY_SECONDS = 15
MAX_DELAY_SECONDS = 45
RETRY_DELAY_SECONDS = 60
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
CACHE_DURATION = 300
MAX_RETRIES = 5
BASE_BACKOFF = 10
MAX_HTML_LENGTH = 8000
MIN_HTML_LENGTH = 500

# ============================================================================
# OLLAMA API FUNCTIONS
# ============================================================================
def ask_ollama(prompt, max_tokens=800, temperature=0.3):
    try:
        print(f"   🤖 Calling Ollama...")
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": max_tokens,
                    "temperature": temperature,
                    "top_p": 0.9,
                    "top_k": 40,
                }
            },
            timeout=120
        )
        if response.status_code == 200:
            result = response.json()
            return result['response']
        else:
            raise Exception(f"Ollama HTTP {response.status_code}: {response.text}")
    except requests.exceptions.Timeout:
        raise Exception("Ollama timeout - model might be too slow for your hardware")
    except requests.exceptions.ConnectionError:
        raise Exception("Cannot connect to Ollama - is it running? (ollama serve)")
    except Exception as e:
        raise Exception(f"Ollama error: {str(e)}")

def verify_ollama():
    try:
        print("\n🔍 Verifying Ollama setup...")
        test_response = ask_ollama("Say OK", max_tokens=10)
        print("✅ Ollama is running")
        print(f"✅ Model: {OLLAMA_MODEL}")
        print("💰 Cost: ₹0 (FREE FOREVER!)")
        print("📊 No rate limits, no quotas")
        print("🚀 Unlimited processing\n")
        return True
    except Exception as e:
        print(f"❌ Ollama verification failed: {e}")
        print("\n📋 SETUP INSTRUCTIONS:")
        print("1. Install: curl -fsSL https://ollama.com/install.sh | sh")
        print("2. Start: ollama serve &")
        print(f"3. Pull model: ollama pull {OLLAMA_MODEL}")
        print("4. Test: ollama run llama3.2:3b 'Hello'")
        print("\n⚠️  For 4GB RAM, llama3.2:3b is the best choice")
        exit(1)

# ============================================================================
# CACHING LAYER
# ============================================================================
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
        self.cache[key] = {'data': data, 'timestamp': time.time()}
        self.save_cache()

cache = SheetsCache()

# ============================================================================
# HTML CLEANING
# ============================================================================
def clean_html_aggressive(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'path', 'meta', 'link', 'head', 'footer', 'nav', 'aside']):
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
    phones = re.findall(r'[\+$$]?[0-9][0-9\s\-$$$$]{8,}[0-9]', text)
    if phones:
        contact['phones'] = list(set([p.strip() for p in phones]))[:3]
    if 'instagram' in text.lower() or '@' in text:
        contact['has_social'] = True
    return contact

# ============================================================================
# SAFE SHEET OPERATIONS
# ============================================================================
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

# ============================================================================
# DAILY TRACKING
# ============================================================================
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

# ============================================================================
# NORMALIZATION & DUPLICATE DETECTION
# ============================================================================
def normalize_text(text):
    if not text:
        return ""
    cleaned = re.sub(r'[^a-z0-9\s]', '', str(text).lower())
    return ' '.join(cleaned.split())

def normalize_phone(phone):
    if not phone:
        return ""
    digits = ''.join(filter(str.isdigit, str(phone)))
    return digits[-10:] if len(digits) >= 10 else digits

def create_duplicate_key(name, phone):
    name_norm = normalize_text(name)
    phone_norm = normalize_phone(phone)
    if phone_norm and len(phone_norm) == 10:
        return f"phone:{phone_norm}"
    if name_norm:
        return f"name:{name_norm}"
    return None

def is_already_processed(restaurant_name, phone_raw):
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Checking duplicates",
            None  # NO CACHE
        )
        new_key = create_duplicate_key(restaurant_name, phone_raw)
        if not new_key:
            return False
        for row in results_data:
            existing_name = str(row.get("Restaurant Name", "")).strip()
            existing_phone = str(row.get("Phone Number", "")).strip()
            existing_key = create_duplicate_key(existing_name, existing_phone)
            if existing_key and new_key == existing_key:
                return True
        return False
    except Exception as e:
        print(f"⚠️  Duplicate check error (allowing): {e}")
        return False

# ============================================================================
# MAIN PROCESSING (OLLAMA + ICE BREAKER)
# ============================================================================
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
            print(f"🔍 Processing: {restaurant_name} (Row {lead_row_index})")
            print(f"{'='*60}")

            # 🔒 DUPLICATE CHECK #1
            if is_already_processed(restaurant_name, phone_raw):
                print(f"⏭️  SKIP: Already in RESULTS")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking duplicate complete"
                )
                continue

            target_url = lead.get("Website URL", "").strip()
            phone_to_save = phone_raw if phone_raw else "No Number"

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, f"Processing... {timestamp}"),
                "Marking as processing"
            )

            # === FALLBACK: NO WEBSITE ===
            if not target_url or target_url.lower() in ["no website found", "", "n/a"]:
                print(f"⚠️  No valid website — using fallback Ice Breaker")
                flaw_analysis = "No website found. Cannot perform analysis."
                builder_prompt = "Create a modern, mobile-friendly website with contact info, menu, and SEO."
                ice_breaker = f"Hi, I noticed {restaurant_name} doesn’t have a website — that’s likely costing you 60%+ of new customers who search online. Can I show you how to fix that in under 24 hours?"

                # 🔒 DUPLICATE CHECK #2
                if is_already_processed(restaurant_name, phone_raw):
                    print(f"❌ DUPLICATE before save — aborting")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                    continue

                # ✅ WRITE ALL 17 COLUMNS
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,         # 1
                        flaw_analysis,           # 2
                        builder_prompt,          # 3
                        "",                      # 4
                        "",                      # 5
                        phone_to_save,           # 6
                        "",                      # 7
                        "",                      # 8
                        "",                      # 9
                        "",                      # 10
                        "",                      # 11
                        "",                      # 12
                        "",                      # 13
                        "",                      # 14
                        "",                      # 15
                        ice_breaker,             # 16 ← ICE BREAKER
                        ""                       # 17
                    ]),
                    "Appending fallback to RESULTS"
                )

                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking complete"
                )
                print(f"✅ Fallback processed: {restaurant_name}")
                print(f"💬 Ice Breaker: {ice_breaker}")
                return True

            # === FULL AI ANALYSIS WITH ICE BREAKER ===
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

            try:
                print(f"🤖 Starting Ollama analysis with urgent Ice Breaker...")
                analysis_prompt = f"""Analyze the website for "{restaurant_name}" and provide:

1. KEY INFORMATION (3-4 bullet points):
   - What the business does
   - Contact info found/missing
   - Main issues

2. FIX CHECKLIST (5-7 actionable items):
   - Missing contact details, broken UX, SEO issues

3. ICE BREAKER (1-2 sentences, URGENT TONE):
   - Write as if you already know them.
   - Imply serious risk (lost customers, bad reviews, competitors winning).
   - Reference something SPECIFIC from their site.
   - Make it feel personal and time-sensitive.

WEBSITE DATA:
{cleaned_html}"""

                full_response = ask_ollama(analysis_prompt, max_tokens=900, temperature=0.3)

                # Parse Ice Breaker
                flaw_analysis = full_response.strip()
                ice_breaker = f"Hi, I was reviewing {restaurant_name} and noticed a critical issue that’s likely costing you customers right now."

                ice_match = re.search(r'ICE BREAKER[:\s]*', full_response, re.IGNORECASE)
                if ice_match:
                    parts = re.split(r'ICE BREAKER[:\s]*', full_response, maxsplit=1, flags=re.IGNORECASE)
                    flaw_analysis = parts[0].strip()
                    ice_candidate = parts[1].strip() if len(parts) > 1 else ""
                    if ice_candidate and len(ice_candidate) > 10:
                        ice_breaker = ice_candidate.split('\n')[0].strip()
                        if not ice_breaker.endswith(('.', '!', '?')):
                            ice_breaker += '.'

                builder_prompt = "Template-based fixes (see analysis)"

                # 🔒 DUPLICATE CHECK #2
                if is_already_processed(restaurant_name, phone_raw):
                    print(f"❌ DUPLICATE before save — aborting")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate complete"
                    )
                    continue

                # ✅ WRITE ALL 17 COLUMNS
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,         # 1
                        flaw_analysis,           # 2
                        builder_prompt,          # 3
                        "",                      # 4
                        "",                      # 5
                        phone_to_save,           # 6
                        "",                      # 7
                        "",                      # 8
                        "",                      # 9
                        "",                      # 10
                        "",                      # 11
                        "",                      # 12
                        "",                      # 13
                        "",                      # 14
                        "",                      # 15
                        ice_breaker,             # 16 ← ICE BREAKER
                        ""                       # 17
                    ]),
                    "Appending AI result to RESULTS"
                )

                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking complete"
                )
                print(f"✅ AI processed: {restaurant_name}")
                print(f"💬 Ice Breaker: {ice_breaker}")
                return True

            except Exception as e:
                print(f"❌ Ollama analysis failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing Error - AI Failed"),
                    "Updating status"
                )
                return False

    print("ℹ️  No pending leads found")
    return False

# ============================================================================
# CONNECT TO SHEETS
# ============================================================================
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

# ============================================================================
# UTILITIES (SYNC & CLEAN)
# ============================================================================
def sync_phone_numbers_from_leads():
    print("\n🔄 === SYNCING PHONE NUMBERS ===")
    try:
        results_data = safe_sheet_read(lambda: results_worksheet.get_all_records(), "Reading RESULTS", None)
        leads_data = safe_sheet_read(lambda: leads_worksheet.get_all_records(), "Reading LEADS", None)
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
                safe_sheet_write(
                    lambda: results_worksheet.update_cell(row_num, 6, correct_phone),
                    f"Syncing phone to F{row_num}"
                )
                updates_made += 1
        print(f"✅ Synced {updates_made} phones" if updates_made else "✅ All phones in sync")
        print("=== SYNC COMPLETE ===\n")
    except Exception as e:
        print(f"❌ Sync error: {e}")

def clean_duplicates_in_results():
    print("\n🧹 === CLEANING DUPLICATES ===")
    try:
        results_data = safe_sheet_read(lambda: results_worksheet.get_all_records(), "Reading RESULTS", None)
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
                continue
            if dup_key in seen:
                rows_to_delete.append(row_num)
            else:
                seen[dup_key] = row_num
        if rows_to_delete:
            for row_num in sorted(rows_to_delete, reverse=True):
                safe_sheet_write(
                    lambda r=row_num: results_worksheet.delete_rows(r),
                    f"Deleting row {row_num}"
                )
        print("✅ Duplicate cleanup done")
        print("=== CLEANUP COMPLETE ===\n")
    except Exception as e:
        print(f"❌ Cleanup error: {e}")

# ============================================================================
# MAIN LOOP
# ============================================================================
verify_ollama()
print("\n" + "="*70)
print("🚀 OLLAMA Lead Processor - SUPERVISED MODE")
print("="*70)
print(f"💎 Model: {OLLAMA_MODEL} (Local)")
print(f"🛡️  Triple duplicate checks")
print(f"👀 Per-lead supervisor")
print(f"📧 Ice Breaker ALWAYS generated")
print(f"💰 Cost: ₹0 FOREVER!")
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
            print(f"   💰 Cost today: ₹0 (FREE!)")
            if remaining > 0:
                delay_seconds = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                print(f"   ⏸️  Waiting {delay_seconds} seconds...")
                print(f"   📅 ETA: ~{int(remaining * (avg_time + delay_seconds) / 60)} minutes\n")
                time.sleep(delay_seconds)
        else:
            print(f"⚠️  No leads. Waiting {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Stopped by user")
        print(f"📊 Processed {daily_log['processed_count']} leads today")
        print(f"💰 Cost: ₹0 (FREE!)")
        break
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"⏸️  Waiting {RETRY_DELAY_SECONDS} seconds...")
        time.sleep(RETRY_DELAY_SECONDS)

print("\n✋ Processor stopped")
print("💰 Total cost: ₹0 (FREE FOREVER!)")
