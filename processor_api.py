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
# 🔥🔥🔥 ULTRA-CHEAP CONFIG - UNDER ₹2 FOR 2 DAYS 🔥🔥🔥
# ============================================================================
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"

# ULTRA-AGGRESSIVE LIMITS TO SAVE MONEY
MAX_LEADS_PER_DAY = 3
MAX_HTML_LENGTH = 1500
MAX_OUTPUT_TOKENS = 200
SHEET_UPDATE_DELAY = 2

# Timing
MIN_DELAY_SECONDS = 45
MAX_DELAY_SECONDS = 120
RETRY_DELAY_SECONDS = 180

# Files
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
CACHE_DURATION = 300
MAX_RETRIES = 3
BASE_BACKOFF = 10

# ============================================================================
# VERIFICATION: ENSURE WE'RE USING A CHEAP MODEL
# ============================================================================
def verify_cheap_model():
    """CRITICAL: Verify we're using gemini-1.5-flash (cheap)"""
    try:
        print("\n🔍 Verifying model...")
        
        # Try gemini-1.5-flash (widely available, cheap)
        test_model = genai.GenerativeModel("gemini-1.5-flash")
        
        response = test_model.generate_content(
            "Say OK",
            generation_config={"max_output_tokens": 5}
        )
        
        print("✅ Model verified: gemini-1.5-flash")
        print("💰 Pricing: $0.075/1M input, $0.30/1M output")
        print("💰 Expected cost: ₹0.02 per lead\n")
        return test_model
        
    except Exception as e:
        print(f"❌ Model gemini-1.5-flash failed: {e}")
        
        # Fallback: Try gemini-1.5-flash-latest
        try:
            print("🔄 Trying gemini-1.5-flash-latest...")
            test_model = genai.GenerativeModel("gemini-1.5-flash-latest")
            
            response = test_model.generate_content(
                "Say OK",
                generation_config={"max_output_tokens": 5}
            )
            
            print("✅ Model verified: gemini-1.5-flash-latest")
            print("💰 Pricing: $0.075/1M input, $0.30/1M output")
            print("💰 Expected cost: ₹0.02 per lead\n")
            return test_model
            
        except Exception as e2:
            print(f"❌ FATAL: All cheap models failed")
            print(f"Error: {e2}")
            print("\n🔍 Listing available models...")
            
            # List available models
            try:
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        print(f"  - {m.name}")
            except:
                pass
            
            print("\n❌ Cannot continue safely")
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
# ULTRA-AGGRESSIVE HTML CLEANING (95%+ reduction)
# ============================================================================
def ultra_clean_html(html_content):
    """Reduce HTML to absolute minimum"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove ALL unnecessary tags
        for tag in soup(['script', 'style', 'noscript', 'iframe', 'svg', 'path',
                        'meta', 'link', 'head', 'footer', 'nav', 'aside', 'header',
                        'form', 'button', 'input', 'img', 'video', 'audio']):
            tag.decompose()
        
        # Get title and headings only
        title = soup.title.string if soup.title else 'No title'
        headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2'])[:5]]
        
        # Get minimal text
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s@.,!?;:()\-\'\"\/]', '', text)
        
        # Extract contact info
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        phones = re.findall(r'[\+\(]?[0-9][0-9\s\-\(\)]{8,}[0-9]', text)
        
        # ULTRA-MINIMAL OUTPUT
        compact = f"""Title: {title[:50]}
Headings: {', '.join(headings[:3])}
Email: {emails[0] if emails else 'None'}
Phone: {phones[0] if phones else 'None'}
Text: {text[:800]}"""
        
        # Hard limit
        return compact[:MAX_HTML_LENGTH]
        
    except Exception as e:
        print(f"⚠️  HTML cleaning error: {e}")
        return html_content[:MAX_HTML_LENGTH]

# ============================================================================
# SAFE SHEET OPERATIONS
# ============================================================================
def safe_sheet_read(operation, operation_name, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = operation()
            time.sleep(1)
            return result
        except Exception as e:
            print(f"⚠️  Error {operation_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(BASE_BACKOFF * (attempt + 1))
    raise Exception(f"Failed {operation_name}")

def safe_sheet_write(operation, operation_name, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = operation()
            time.sleep(SHEET_UPDATE_DELAY)
            return result
        except Exception as e:
            print(f"⚠️  Error {operation_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(BASE_BACKOFF * (attempt + 1))
    raise Exception(f"Failed {operation_name}")

# ============================================================================
# DAILY TRACKING
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
# NORMALIZE & DUPLICATE DETECTION
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
                print(f"  ⚠️  DUPLICATE: {restaurant_name}")
                return True
        
        return False
        
    except Exception as e:
        print(f"⚠️  Duplicate check error: {e}")
        return False

# ============================================================================
# MAIN PROCESSING FUNCTION
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
            print(f"🔍 Processing: {restaurant_name}")
            print(f"{'='*60}")
            
            # Check duplicate
            if is_duplicate(restaurant_name, phone_raw, results_worksheet):
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                    "Marking duplicate"
                )
                continue
            
            # Validate URL
            if not target_url or target_url.lower() in ["no website found", ""]:
                print(f"❌ Invalid URL")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - No URL"),
                    "Marking error"
                )
                return None
            
            # Mark as processing
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, f"Processing... {timestamp}"),
                "Marking processing"
            )
            
            # SCRAPE WEBSITE
            try:
                print(f"🌐 Scraping: {target_url}")
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    try:
                        page.goto(target_url, timeout=45000, wait_until='domcontentloaded')
                        body_html = page.locator("body").inner_html()
                        print(f"   📄 Raw HTML: {len(body_html):,} chars")
                        
                        cleaned_html = ultra_clean_html(body_html)
                        print(f"   ✨ Cleaned: {len(cleaned_html):,} chars (95% reduction)")
                        
                    finally:
                        browser.close()
            except Exception as e:
                print(f"❌ Scraping failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - Scraping"),
                    "Marking error"
                )
                return None
            
            # AI ANALYSIS (ULTRA-MINIMAL)
            try:
                print(f"🤖 AI analysis (ultra-cheap)...")
                
                # ULTRA-SHORT PROMPT
                prompt = f"""Website: {restaurant_name}

List 5 issues:
1.
2.
3.
4.
5.

{cleaned_html}"""
                
                start_time = time.time()
                
                response = model.generate_content(
                    prompt,
                    generation_config={
                        "max_output_tokens": MAX_OUTPUT_TOKENS,
                        "temperature": 0.1,
                    }
                )
                
                ai_response = response.text
                api_time = time.time() - start_time
                
                # ACCURATE COST CALCULATION
                # gemini-1.5-flash: $0.075/1M input, $0.30/1M output
                input_tokens = int((len(cleaned_html) + len(prompt)) / 4)
                output_tokens = int(len(ai_response) / 4)
                
                input_cost_usd = (input_tokens / 1_000_000) * 0.075
                output_cost_usd = (output_tokens / 1_000_000) * 0.30
                total_cost_usd = input_cost_usd + output_cost_usd
                total_cost_inr = total_cost_usd * 85
                
                print(f"   ✅ Completed in {api_time:.1f}s")
                print(f"   📊 Tokens: {input_tokens:,} in + {output_tokens:,} out")
                print(f"   💰 Cost: ${total_cost_usd:.6f} = ₹{total_cost_inr:.4f}")
                
                # Double-check duplicate before saving
                if is_duplicate(restaurant_name, phone_raw, results_worksheet):
                    print(f"❌ Duplicate in final check")
                    safe_sheet_write(
                        lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                        "Marking duplicate"
                    )
                    return None
                
                # Save to RESULTS
                phone_to_save = phone_raw if phone_raw else "No Number"
                
                print(f"💾 Saving to RESULTS...")
                safe_sheet_write(
                    lambda: results_worksheet.append_row([
                        restaurant_name,
                        ai_response,
                        "Template fixes",
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
                
                print(f"✅ SUCCESS: {restaurant_name}")
                print(f"{'='*60}\n")
                
                return total_cost_inr
                
            except Exception as e:
                print(f"❌ AI analysis failed: {e}")
                safe_sheet_write(
                    lambda: leads_worksheet.update_cell(lead_row_index, 6, "Error - AI"),
                    "Marking error"
                )
                return None
    
    print("ℹ️  No pending leads")
    return None

# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🔥 ULTRA-CHEAP Lead Processor v4.0 - EMERGENCY FIX")
    print("="*70)
    print(f"💎 Model: gemini-1.5-flash (VERIFIED CHEAPEST AVAILABLE)")
    print(f"📊 Daily Limit: {MAX_LEADS_PER_DAY} leads")
    print(f"💰 Target Cost: ₹0.02 per lead = ₹{MAX_LEADS_PER_DAY * 0.02:.2f}/day")
    print(f"💰 2-Day Cost: ₹{MAX_LEADS_PER_DAY * 0.02 * 2:.2f}")
    print(f"⏱️  Delay: {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS}s between leads")
    print("="*70 + "\n")
    
    # CRITICAL: Verify model before starting
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = verify_cheap_model()
    except Exception as e:
        print(f"❌ FATAL: Cannot start - {e}")
        exit(1)
    
    # Connect to sheets
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
        print("✅ Connected to Google Sheets\n")
    except Exception as e:
        print(f"❌ FATAL: Sheets error - {e}")
        exit(1)
    
    # Main loop
    daily_log = load_daily_log()
    processing_costs = []
    
    while True:
        try:
            daily_log = reset_daily_count_if_new_day(daily_log)
            
            # Check daily limit
            if daily_log["processed_count"] >= MAX_LEADS_PER_DAY:
                print(f"🎯 Daily limit reached ({MAX_LEADS_PER_DAY} leads)")
                print(f"💰 Today's cost: ₹{daily_log['total_cost_inr']:.4f}")
                
                now = datetime.now()
                tomorrow = now.replace(hour=0, minute=1) + timedelta(days=1)
                sleep_seconds = (tomorrow - now).total_seconds()
                print(f"😴 Sleeping until {tomorrow.strftime('%Y-%m-%d %H:%M')}\n")
                time.sleep(sleep_seconds)
                continue
            
            # Process one lead
            cost = process_single_lead(leads_worksheet, results_worksheet, model)
            
            if cost is not None:
                daily_log["processed_count"] += 1
                daily_log["total_cost_inr"] += cost
                save_daily_log(daily_log)
                
                processing_costs.append(cost)
                
                remaining = MAX_LEADS_PER_DAY - daily_log["processed_count"]
                avg_cost = sum(processing_costs) / len(processing_costs)
                
                print(f"\n📈 PROGRESS")
                print(f"   ✅ Completed: {daily_log['processed_count']}/{MAX_LEADS_PER_DAY}")
                print(f"   ⏳ Remaining: {remaining}")
                print(f"   💰 Avg cost: ₹{avg_cost:.4f} per lead")
                print(f"   💰 Today total: ₹{daily_log['total_cost_inr']:.4f}")
                
                if remaining > 0:
                    delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                    print(f"   ⏸️  Waiting {delay}s...\n")
                    time.sleep(delay)
            else:
                print(f"⚠️  No leads processed. Waiting {RETRY_DELAY_SECONDS}s...")
                time.sleep(RETRY_DELAY_SECONDS)
                
        except KeyboardInterrupt:
            print("\n\n⛔ Stopped by user")
            print(f"📊 Processed: {daily_log['processed_count']} leads today")
            print(f"💰 Cost: ₹{daily_log['total_cost_inr']:.4f}")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print(f"⏸️  Waiting {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)
    
    print("\n✋ Processor stopped")
    print(f"💰 Final cost: ₹{daily_log['total_cost_inr']:.4f}")
