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

# ============================================================================
# 🔥 ULTRA-MINIMAL CONFIG - PENNIES PER DAY 🔥
# ============================================================================
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"

# ULTRA-AGGRESSIVE COST REDUCTION
MAX_LEADS_PER_DAY = 5
MAX_HTML_LENGTH = 800  # SUPER SHORT - just enough for issues
MAX_OUTPUT_TOKENS = 150  # MINIMAL output
SHEET_UPDATE_DELAY = 2

# Timing
MIN_DELAY_SECONDS = 30
MAX_DELAY_SECONDS = 90
RETRY_DELAY_SECONDS = 120

# Files
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
CACHE_DURATION = 300
MAX_RETRIES = 3
BASE_BACKOFF = 10

# ============================================================================
# MODEL VERIFICATION
# ============================================================================
def verify_cheap_model():
    """Use gemini-2.5-flash-lite-preview (cheapest 2.5 model)"""
    try:
        print("\n🔍 Verifying model...")
        
        # Use the cheapest 2.5 model from your API
        test_model = genai.GenerativeModel("gemini-2.5-flash-lite-preview-09-2025")
        
        response = test_model.generate_content(
            "OK",
            generation_config={"max_output_tokens": 5}
        )
        
        print("✅ Model verified: gemini-2.5-flash-lite-preview")
        print("💰 Ultra-minimal prompt strategy")
        print("💰 Expected cost: <₹0.01 per lead\n")
        return test_model
        
    except Exception as e:
        print(f"❌ Model verification failed: {e}")
        print("\n🔄 Trying gemini-2.5-flash-preview...")
        
        try:
            test_model = genai.GenerativeModel("gemini-2.5-flash-preview-09-2025")
            response = test_model.generate_content(
                "OK",
                generation_config={"max_output_tokens": 5}
            )
            print("✅ Model verified: gemini-2.5-flash-preview")
            return test_model
        except Exception as e2:
            print(f"❌ FATAL: {e2}")
            exit(1)

# ============================================================================
# CACHING
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
                return cached_data['data']
        return None
    
    def set(self, key, data):
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
        self.save_cache()

cache = SheetsCache()

# ============================================================================
# ULTRA-MINIMAL HTML EXTRACTION (just extract key info)
# ============================================================================
def extract_minimal_info(html_content):
    """Extract ONLY what's needed - title, contact, key text"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove junk
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 
                        'meta', 'link', 'head', 'footer', 'nav', 'aside']):
            tag.decompose()
        
        # Get basics
        title = soup.title.string if soup.title else 'No title'
        h1_tags = [h.get_text(strip=True) for h in soup.find_all('h1')[:2]]
        
        # Get text
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        
        # Find contact
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        phones = re.findall(r'[\+\(]?[0-9][0-9\s\-\(\)]{8,}[0-9]', text)
        
        # ULTRA-SHORT OUTPUT
        output = f"""T:{title[:40]}
H:{h1_tags[0][:40] if h1_tags else 'None'}
E:{'Yes' if emails else 'No'}
P:{'Yes' if phones else 'No'}
TXT:{text[:500]}"""
        
        return output[:MAX_HTML_LENGTH]
        
    except Exception as e:
        return html_content[:MAX_HTML_LENGTH]

# ============================================================================
# SAFE SHEET OPS
# ============================================================================
def safe_sheet_read(operation, operation_name, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = operation()
            time.sleep(1)
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(BASE_BACKOFF)
    raise Exception(f"Failed {operation_name}")

def safe_sheet_write(operation, operation_name, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = operation()
            time.sleep(SHEET_UPDATE_DELAY)
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(BASE_BACKOFF)
    raise Exception(f"Failed {operation_name}")

# ============================================================================
# TRACKING
# ============================================================================
def load_daily_log():
    if os.path.exists(TRACKING_FILE):
        try:
            with open(TRACKING_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"date": "", "processed_count": 0, "total_cost_inr": 0.0}

def save_daily_log(data):
    with open(TRACKING_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def reset_daily_count_if_new_day(log_data):
    today = datetime.now().strftime("%Y-%m-%d")
    if log_data["date"] != today:
        log_data["date"] = today
        log_data["processed_count"] = 0
        log_data["total_cost_inr"] = 0.0
        save_daily_log(log_data)
    return log_data

# ============================================================================
# DUPLICATE DETECTION
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
    phone_norm = normalize_phone(phone)
    if phone_norm and len(phone_norm) == 10:
        return f"phone:{phone_norm}"
    name_norm = normalize_text(name)
    if name_norm:
        return f"name:{name_norm}"
    return None

def is_duplicate(restaurant_name, phone_raw, results_worksheet):
    try:
        results_data = safe_sheet_read(
            lambda: results_worksheet.get_all_records(),
            "Checking duplicates"
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
    except:
        return False

# ============================================================================
# MAIN PROCESSING
# ============================================================================
def process_single_lead(leads_worksheet, results_worksheet, model):
    try:
        all_leads = safe_sheet_read(
            lambda: leads_worksheet.get_all_records(),
            "Fetching LEADS"
        )
    except Exception as e:
        print(f"❌ Failed to fetch leads: {e}")
        return None
    
    for idx, lead in enumerate(all_leads):
        status = str(lead.get("Status", "")).strip().lower()
        
        if status == "pending":
            lead_row_index = idx + 2
            restaurant_name = str(lead.get("Restaurant Name", "")).strip()
            phone_raw = str(lead.get("Phone Number", "")).strip()
            target_url = lead.get("Website URL", "").strip()
            
            print(f"\n{'='*60}")
            print(f"🔍 {restaurant_name}")
            print(f"{'='*60}")
            
            # Check duplicate
            if is_duplicate(restaurant_name, phone_raw, results_worksheet):
                print(f"⏭️  SKIP: Duplicate")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Mark dup"
                )
                continue
            
            # Validate URL
            if not target_url or target_url.lower() in ["no website found", ""]:
                print(f"❌ No URL")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - No URL"),
                    "Mark error"
                )
                return None
            
            # Mark processing
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, "Processing..."),
                "Mark processing"
            )
            
            # SCRAPE
            try:
                print(f"🌐 Scraping...")
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    try:
                        page.goto(target_url, timeout=40000, wait_until='domcontentloaded')
                        body_html = page.locator("body").inner_html()
                        
                        minimal_info = extract_minimal_info(body_html)
                        print(f"   📄 Extracted: {len(minimal_info)} chars")
                        
                    finally:
                        browser.close()
            except Exception as e:
                print(f"❌ Scraping failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - Scraping"),
                    "Mark error"
                )
                return None
            
            # AI ANALYSIS (ABSOLUTE MINIMUM)
            try:
                print(f"🤖 AI check...")
                
                # ULTRA-MINIMAL PROMPT (just find problems)
                prompt = f"""Quick check for {restaurant_name}:
List 3-5 issues (missing email, broken links, etc):

{minimal_info}"""
                
                start_time = time.time()
                
                response = model.generate_content(
                    prompt,
                    generation_config={
                        "max_output_tokens": MAX_OUTPUT_TOKENS,
                        "temperature": 0,
                    }
                )
                
                issues = response.text
                api_time = time.time() - start_time
                
                # COST (estimate based on flash-lite pricing ~$0.05/$0.20 per 1M)
                input_tokens = int((len(minimal_info) + len(prompt)) / 4)
                output_tokens = int(len(issues) / 4)
                
                # Conservative estimate for flash-lite
                input_cost_usd = (input_tokens / 1_000_000) * 0.05
                output_cost_usd = (output_tokens / 1_000_000) * 0.20
                total_cost_usd = input_cost_usd + output_cost_usd
                total_cost_inr = total_cost_usd * 85
                
                print(f"   ✅ Done in {api_time:.1f}s")
                print(f"   📊 ~{input_tokens:,} in + {output_tokens:,} out")
                print(f"   💰 ₹{total_cost_inr:.4f}")
                
                # Final dup check
                if is_duplicate(restaurant_name, phone_raw, results_worksheet):
                    print(f"❌ Dup in final check")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Mark dup"
                    )
                    return None
                
                # Save
                phone_to_save = phone_raw if phone_raw else "No Number"
                
                print(f"💾 Saving...")
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,
                        issues,
                        "Quick fixes needed",
                        "",
                        "",
                        phone_to_save
                    ]),
                    "Save to RESULTS"
                )
                
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Mark done"
                )
                
                print(f"✅ DONE")
                print(f"{'='*60}\n")
                
                return total_cost_inr
                
            except Exception as e:
                print(f"❌ AI failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - AI"),
                    "Mark error"
                )
                return None
    
    print("ℹ️  No pending leads")
    return None

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🔥 ULTRA-CHEAP Lead Processor - MINIMAL VERSION")
    print("="*70)
    print(f"💎 Model: gemini-2.5-flash-lite-preview")
    print(f"📊 Limit: {MAX_LEADS_PER_DAY} leads/day")
    print(f"💰 Strategy: Minimal prompts + short outputs")
    print(f"💰 Target: <₹0.50 for 2 days")
    print("="*70 + "\n")
    
    # Verify model
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = verify_cheap_model()
    except Exception as e:
        print(f"❌ FATAL: {e}")
        exit(1)
    
    # Connect sheets
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
        print("✅ Connected to Sheets\n")
    except Exception as e:
        print(f"❌ FATAL: {e}")
        exit(1)
    
    # Main loop
    daily_log = load_daily_log()
    costs = []
    
    while True:
        try:
            daily_log = reset_daily_count_if_new_day(daily_log)
            
            # Check limit
            if daily_log["processed_count"] >= MAX_LEADS_PER_DAY:
                print(f"🎯 Limit reached ({MAX_LEADS_PER_DAY})")
                print(f"💰 Today: ₹{daily_log['total_cost_inr']:.4f}")
                
                now = datetime.now()
                tomorrow = now.replace(hour=0, minute=1) + timedelta(days=1)
                sleep_seconds = (tomorrow - now).total_seconds()
                print(f"😴 Sleep until {tomorrow.strftime('%H:%M')}\n")
                time.sleep(sleep_seconds)
                continue
            
            # Process
            cost = process_single_lead(leads_worksheet, results_worksheet, model)
            
            if cost is not None:
                daily_log["processed_count"] += 1
                daily_log["total_cost_inr"] += cost
                save_daily_log(daily_log)
                costs.append(cost)
                
                remaining = MAX_LEADS_PER_DAY - daily_log["processed_count"]
                avg = sum(costs) / len(costs)
                
                print(f"\n📈 {daily_log['processed_count']}/{MAX_LEADS_PER_DAY} | Avg: ₹{avg:.4f} | Total: ₹{daily_log['total_cost_inr']:.4f}")
                
                if remaining > 0:
                    delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                    print(f"⏸️  {delay}s...\n")
                    time.sleep(delay)
            else:
                print(f"⏸️  {RETRY_DELAY_SECONDS}s...")
                time.sleep(RETRY_DELAY_SECONDS)
                
        except KeyboardInterrupt:
            print(f"\n⛔ Stopped")
            print(f"💰 ₹{daily_log['total_cost_inr']:.4f}")
            break
        except Exception as e:
            print(f"❌ {e}")
            time.sleep(RETRY_DELAY_SECONDS)
    
    print(f"\n✋ Final: ₹{daily_log['total_cost_inr']:.4f}")
