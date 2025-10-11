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
from typing import Dict, Any, Optional, List, Tuple
from enum import Enum
from dataclasses import dataclass
import unicodedata

# ============================================================================
# 🔥 CONFIGURATION
# ============================================================================
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2:3b"
SPREADSHEET_NAME = "Lead Gen Engine"
SHEET_UPDATE_DELAY = 3
MAX_LEADS_PER_DAY = 50
MIN_DELAY_SECONDS = 15
MAX_DELAY_SECONDS = 45
RETRY_DELAY_SECONDS = 60
REST_AFTER_LEADS = 10
REST_DURATION = 300

# Files
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
SUPERVISOR_LOG_FILE = "supervisor_decisions.jsonl"
DUPLICATE_REGISTRY_FILE = "duplicate_registry.json"
PHONE_SYNC_LOG_FILE = "phone_sync_log.json"
PROGRESS_FILE = "progress_tracker.json"
HEALTH_CHECK_FILE = "system_health.json"

# Limits
CACHE_DURATION = 300
MAX_RETRIES = 5
BASE_BACKOFF = 10
MAX_HTML_LENGTH = 8000
MIN_HTML_LENGTH = 500

# ============================================================================
# 📊 CORE TYPES
# ============================================================================
class TaskStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    FALLBACK_USED = "fallback_used"
    RETRY_NEEDED = "retry_needed"
    CATASTROPHIC = "catastrophic"
    BLOCKED = "blocked"

@dataclass
class LeadData:
    """Complete lead data structure"""
    restaurant_name: str
    phone: str
    website_url: str
    flaw_analysis: str
    builder_prompt: str
    preview_url: str
    ice_breaker: str
    row_index: int
    
    def to_sheet_row(self) -> List[str]:
        """Convert to sheet row format (17 columns)"""
        return [
            self.restaurant_name,
            self.flaw_analysis,
            self.builder_prompt,
            "",
            self.preview_url,
            self.phone,
            "", "", "", "", "", "", "", "", "",
            self.ice_breaker,
            ""
        ]

# ============================================================================
# 📝 SUPERVISOR DECISION LOGGER
# ============================================================================
class SupervisorLogger:
    """Centralized logging for all supervisors"""
    
    def __init__(self):
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def log(self, supervisor: str, phase: str, status: TaskStatus, 
            details: str, data: Dict = None):
        """Log a supervisor decision"""
        log_entry = {
            "session": self.session_id,
            "timestamp": datetime.now().isoformat(),
            "supervisor": supervisor,
            "phase": phase,
            "status": status.value,
            "details": details,
            "data": data or {}
        }
        
        icons = {
            TaskStatus.SUCCESS: "✅",
            TaskStatus.FAILED: "❌",
            TaskStatus.FALLBACK_USED: "🔄",
            TaskStatus.RETRY_NEEDED: "⚠️",
            TaskStatus.CATASTROPHIC: "🔥",
            TaskStatus.BLOCKED: "🚫"
        }
        icon = icons.get(status, "ℹ️")
        print(f"{icon} [{supervisor}:{phase}] {details}")
        
        try:
            with open(SUPERVISOR_LOG_FILE, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"⚠️  Log write failed: {e}")

LOGGER = SupervisorLogger()

# ============================================================================
# 🤖 OLLAMA FUNCTIONS
# ============================================================================
def ask_ollama(prompt, max_tokens=800, temperature=0.3):
    """Call Ollama API"""
    try:
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
            raise Exception(f"Ollama HTTP {response.status_code}")
    except requests.exceptions.Timeout:
        raise Exception("Ollama timeout")
    except requests.exceptions.ConnectionError:
        raise Exception("Cannot connect to Ollama")
    except Exception as e:
        raise Exception(f"Ollama error: {str(e)}")

def verify_ollama():
    """Verify Ollama is running"""
    try:
        print("\n🔍 Verifying Ollama setup...")
        test_response = ask_ollama("Say OK", max_tokens=10)
        print("✅ Ollama is running")
        print(f"✅ Model: {OLLAMA_MODEL}")
        print("💰 Cost: ₹0 (FREE FOREVER!)")
        return True
    except Exception as e:
        print(f"❌ Ollama verification failed: {e}")
        print("\n📋 SETUP INSTRUCTIONS:")
        print("1. Install: curl -fsSL https://ollama.com/install.sh | sh")
        print("2. Start: ollama serve &")
        print(f"3. Pull model: ollama pull {OLLAMA_MODEL}")
        exit(1)

# ============================================================================
# 🗄️ CACHING LAYER
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
        self.cache[key] = {'data': data, 'timestamp': time.time()}
        self.save_cache()

cache = SheetsCache()

# ============================================================================
# 🧹 HTML CLEANING
# ============================================================================
def clean_html_aggressive(html_content):
    """Clean HTML aggressively"""
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
        LOGGER.log("HTMLCleaner", "error", TaskStatus.FAILED, f"Cleaning failed: {e}")
        return html_content[:MAX_HTML_LENGTH]

def extract_contact_info(text):
    """Extract contact info from text"""
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

# ============================================================================
# 🔤 ICE BREAKER EXTRACTION (GPT-5 ENHANCED)
# ============================================================================
ICE_BREAKER_HEADER_RE = re.compile(
    r'^\s*(?:\d+\s*[\).:-]\s*)?(?:ice[\s\-]*breaker|icebreaker)\b.*$',
    flags=re.IGNORECASE | re.MULTILINE
)

def extract_ice_breaker(full_text: str) -> str:
    """Extract ice breaker from AI response"""
    match = ICE_BREAKER_HEADER_RE.search(full_text)
    if match:
        after = full_text[match.end():]
        for line in after.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            cleaned = re.sub(r'^[\-\*\u2022]\s*', '', cleaned).strip()
            if cleaned and len(cleaned) > 12:
                if not cleaned.endswith(('.', '!', '?')):
                    cleaned += '.'
                return cleaned[:350]
    
    # Fallback: Look for sentences with urgency keywords
    for line in full_text.splitlines():
        candidate = line.strip()
        if (12 <= len(candidate) <= 350 and 
            candidate[0].isupper() and 
            candidate.endswith(('.', '!', '?')) and
            any(word in candidate.lower() for word in 
                ['noticed', 'spotted', 'see', 'found', 'missing', 'losing', 'costing'])):
            return candidate
    
    return ""

def generate_site_ice_breaker(restaurant_name: str, cleaned_html: str, preview_url: str) -> str:
    """Generate fallback ice breaker for websites (ULTRA-SOLID)"""
    title_match = re.search(r'^TITLE:\s*(.+)$', cleaned_html, flags=re.MULTILINE)
    title = title_match.group(1).strip() if title_match else restaurant_name
    
    if len(title) > 50:
        title = restaurant_name
    
    # Check for missing contact info
    has_email = '@' in cleaned_html or 'email' in cleaned_html.lower()
    has_phone = bool(re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', cleaned_html))
    
    if not has_email and not has_phone:
        specific_issue = "your site is missing contact info"
    elif not has_email:
        specific_issue = "potential customers can't find your email"
    elif not has_phone:
        specific_issue = "your phone number isn't visible"
    else:
        specific_issue = "your site needs mobile optimization"
    
    return (
        f"Quick note after reviewing {title}—I noticed {specific_issue}. "
        f"This is likely costing you customers. Preview: {preview_url} "
        f"Can we discuss a 24-hour fix?"
    )

def generate_fallback_ice_breaker(restaurant_name: str, preview_url: str) -> str:
    """Generate fallback for NO website (ULTRA-SOLID)"""
    return (
        f"Hi, I noticed {restaurant_name} doesn't have a website—that's costing you 60%+ of potential customers "
        f"who search online. I've created a preview of what your site could look like: {preview_url} "
        f"Can I show you how to launch in 24 hours?"
    )

# ============================================================================
# 🔗 ASCII SLUGGING (GPT-5 SUGGESTION)
# ============================================================================
def slug_ascii(text: str) -> str:
    """Convert to ASCII-safe slug"""
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-z0-9]+', '-', normalized.lower()).strip('-')

# ============================================================================
# 🛡️ SAFE SHEET OPERATIONS
# ============================================================================
def safe_sheet_read(operation, operation_name, cache_key=None, max_retries=MAX_RETRIES):
    """Safe read with caching"""
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
                LOGGER.log("SheetReader", "rate_limit", TaskStatus.RETRY_NEEDED,
                          f"Rate limit hit. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            LOGGER.log("SheetReader", "error", TaskStatus.FAILED, f"{operation_name}: {e}")
            time.sleep(BASE_BACKOFF)
    
    raise Exception(f"Failed {operation_name} after {max_retries} attempts")

def safe_sheet_write(operation, operation_name, max_retries=MAX_RETRIES):
    """Safe write with retries"""
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
                LOGGER.log("SheetWriter", "rate_limit", TaskStatus.RETRY_NEEDED,
                          f"Rate limit hit. Waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                time.sleep(BASE_BACKOFF)
        except Exception as e:
            LOGGER.log("SheetWriter", "error", TaskStatus.FAILED, f"{operation_name}: {e}")
            time.sleep(BASE_BACKOFF)
    
    raise Exception(f"Failed {operation_name} after {max_retries} attempts")

# ============================================================================
# 🔤 TEXT NORMALIZATION
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

# ============================================================================
# 🛡️ DUPLICATE GUARDIAN - 3-PHASE PROTECTION
# ============================================================================
class DuplicateGuardian:
    """Triple-layer duplicate prevention"""
    
    def __init__(self):
        self.registry = self._load_registry()
    
    def _load_registry(self) -> Dict:
        if os.path.exists(DUPLICATE_REGISTRY_FILE):
            try:
                with open(DUPLICATE_REGISTRY_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {"keys": {}, "last_updated": None}
    
    def _save_registry(self):
        self.registry["last_updated"] = datetime.now().isoformat()
        with open(DUPLICATE_REGISTRY_FILE, 'w') as f:
            json.dump(self.registry, f, indent=2)
    
    def _create_duplicate_key(self, name: str, phone: str) -> str:
        name_norm = re.sub(r'[^a-z0-9]', '', name.lower())
        phone_norm = ''.join(filter(str.isdigit, phone))[-10:] if phone else ""
        
        if phone_norm and len(phone_norm) == 10:
            key = f"phone:{phone_norm}"
        elif name_norm:
            key = f"name:{name_norm}"
        else:
            key = None
        
        return key
    
    def phase1_check_before(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """Phase 1: Check BEFORE processing"""
        LOGGER.log("DuplicateGuardian", "phase1_start", TaskStatus.SUCCESS,
                   f"Phase 1 check for {name}")
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            return False, "no_key"
        
        if dup_key in self.registry["keys"]:
            LOGGER.log("DuplicateGuardian", "phase1_registry_hit", TaskStatus.BLOCKED,
                       f"Found in registry: {dup_key}")
            return True, "registry"
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase1 sheet check",
                None
            )
            
            for row in results_data:
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    LOGGER.log("DuplicateGuardian", "phase1_sheet_hit", TaskStatus.BLOCKED,
                               f"Found in sheet: {existing_name}")
                    self.registry["keys"][dup_key] = {
                        "name": existing_name,
                        "phone": existing_phone,
                        "added": datetime.now().isoformat()
                    }
                    self._save_registry()
                    return True, "sheet"
        
        except Exception as e:
            LOGGER.log("DuplicateGuardian", "phase1_check_error", TaskStatus.FAILED,
                       f"Sheet check failed: {e}")
        
        LOGGER.log("DuplicateGuardian", "phase1_passed", TaskStatus.SUCCESS,
                   f"Phase 1 passed for {name}")
        return False, "passed"
    
    def phase2_check_during(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """Phase 2: Check DURING processing"""
        LOGGER.log("DuplicateGuardian", "phase2_start", TaskStatus.SUCCESS,
                   f"Phase 2 check for {name}")
        
        time.sleep(2)
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            return False, "no_key"
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase2 sheet check",
                None
            )
            
            for row in results_data:
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    LOGGER.log("DuplicateGuardian", "phase2_duplicate", TaskStatus.BLOCKED,
                               f"Duplicate detected: {existing_name}")
                    return True, "concurrent"
            
            return False, "passed"
            
        except Exception as e:
            LOGGER.log("DuplicateGuardian", "phase2_error", TaskStatus.FAILED,
                       f"Phase 2 check failed: {e}")
            return False, "error"
    
    def phase3_verify_after(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """Phase 3: Verify AFTER save"""
        LOGGER.log("DuplicateGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Phase 3 verification for {name}")
        
        time.sleep(3)
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            return True, "no_key"
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase3 sheet check",
                None
            )
            
            matches = []
            for idx, row in enumerate(results_data):
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    matches.append((idx + 2, existing_name, existing_phone))
            
            if len(matches) == 0:
                LOGGER.log("DuplicateGuardian", "phase3_missing", TaskStatus.CATASTROPHIC,
                           "Entry not found after save!")
                return False, "missing"
            
            elif len(matches) == 1:
                LOGGER.log("DuplicateGuardian", "phase3_success", TaskStatus.SUCCESS,
                           f"Verified single entry for {name}")
                self.registry["keys"][dup_key] = {
                    "name": name,
                    "phone": phone,
                    "added": datetime.now().isoformat()
                }
                self._save_registry()
                return True, "verified"
            
            else:
                LOGGER.log("DuplicateGuardian", "phase3_duplicates_found", TaskStatus.CATASTROPHIC,
                           f"Found {len(matches)} duplicates!")
                
                for row_num, _, _ in matches[1:]:
                    try:
                        results_worksheet.delete_rows(row_num)
                        LOGGER.log("DuplicateGuardian", "phase3_cleanup", TaskStatus.SUCCESS,
                                   f"Deleted duplicate at row {row_num}")
                        time.sleep(2)
                    except Exception as e:
                        LOGGER.log("DuplicateGuardian", "phase3_cleanup_failed", TaskStatus.FAILED,
                                   f"Failed to delete row {row_num}: {e}")
                
                return True, "cleaned"
        
        except Exception as e:
            LOGGER.log("DuplicateGuardian", "phase3_error", TaskStatus.FAILED,
                       f"Phase 3 verification failed: {e}")
            return False, "error"

# ============================================================================
# 📞 PHONE SYNC GUARDIAN
# ============================================================================
class PhoneSyncGuardian:
    """Ensures phone numbers are always in sync"""
    
    def __init__(self):
        self.phone_map = {}
    
    def _normalize_name(self, name: str) -> str:
        return re.sub(r'[^a-z0-9]', '', name.lower())
    
    def phase1_build_map(self, leads_worksheet):
        """Phase 1: Build phone map"""
        LOGGER.log("PhoneSyncGuardian", "phase1_start", TaskStatus.SUCCESS,
                   "Building phone map")
        
        try:
            leads_data = safe_sheet_read(
                lambda: leads_worksheet.get_all_records(),
                "Phase1 build phone map",
                None
            )
            
            self.phone_map = {}
            for lead in leads_data:
                name = str(lead.get("Restaurant Name", "")).strip()
                phone = str(lead.get("Phone Number", "")).strip()
                
                if name:
                    name_norm = self._normalize_name(name)
                    self.phone_map[name_norm] = phone if phone else "No Number"
            
            LOGGER.log("PhoneSyncGuardian", "phase1_complete", TaskStatus.SUCCESS,
                       f"Built map with {len(self.phone_map)} entries")
            
        except Exception as e:
            LOGGER.log("PhoneSyncGuardian", "phase1_error", TaskStatus.FAILED,
                       f"Failed to build map: {e}")
    
    def phase2_get_correct_phone(self, name: str, provided_phone: str) -> str:
        """Phase 2: Get correct phone"""
        name_norm = self._normalize_name(name)
        
        if name_norm in self.phone_map:
            correct_phone = self.phone_map[name_norm]
            
            if correct_phone != provided_phone:
                LOGGER.log("PhoneSyncGuardian", "phase2_correction", TaskStatus.FALLBACK_USED,
                           f"Correcting phone: {provided_phone} → {correct_phone}")
            
            return correct_phone
        else:
            return provided_phone if provided_phone else "No Number"
    
    def phase3_verify_sync(self, name: str, expected_phone: str, results_worksheet) -> bool:
        """Phase 3: Verify phone"""
        LOGGER.log("PhoneSyncGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Verifying phone for {name}")
        
        time.sleep(2)
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase3 phone verify",
                None
            )
            
            for idx, row in enumerate(results_data):
                row_name = str(row.get("Restaurant Name", "")).strip()
                
                if self._normalize_name(row_name) == self._normalize_name(name):
                    saved_phone = str(row.get("Phone Number", "")).strip()
                    
                    if saved_phone == expected_phone:
                        LOGGER.log("PhoneSyncGuardian", "phase3_verified", TaskStatus.SUCCESS,
                                   f"Phone verified: {saved_phone}")
                        return True
                    else:
                        row_num = idx + 2
                        try:
                            results_worksheet.update_cell(row_num, 6, expected_phone)
                            LOGGER.log("PhoneSyncGuardian", "phase3_fixed", TaskStatus.FALLBACK_USED,
                                       f"Fixed phone at row {row_num}")
                            return True
                        except Exception as e:
                            LOGGER.log("PhoneSyncGuardian", "phase3_fix_failed", TaskStatus.CATASTROPHIC,
                                       f"Failed to fix: {e}")
                            return False
            
            return False
            
        except Exception as e:
            LOGGER.log("PhoneSyncGuardian", "phase3_error", TaskStatus.FAILED,
                       f"Verification failed: {e}")
            return False

# ============================================================================
# 🔗 PREVIEW URL GUARDIAN (GPT-5 ENHANCED)
# ============================================================================
class PreviewURLGuardian:
    """Ensures preview URL is always generated and embedded"""
    
    BASE_URL = "https://lead-gen-engine.vercel.app"
    
    def phase1_generate(self, name: str) -> str:
        """Phase 1: Generate URL with ASCII-safe slug"""
        project_id = slug_ascii(name)
        url = f"{self.BASE_URL}/?client={project_id}"
        
        LOGGER.log("PreviewURLGuardian", "phase1_generated", TaskStatus.SUCCESS,
                   f"Generated URL: {url}")
        
        return url
    
    def phase2_embed_in_icebreaker(self, ice_breaker: str, preview_url: str) -> str:
        """Phase 2: Embed URL (GPT-5 HARDENED)"""
        
        base = ice_breaker.strip()
        
        # Handle empty ice breaker
        if not base:
            base = "Quick note after reviewing your site—I can share a 24-hour plan to boost conversions."
            LOGGER.log("PreviewURLGuardian", "phase2_empty_handled", TaskStatus.FALLBACK_USED,
                       "Generated fallback for empty ice breaker")
        
        # Already has URL
        if preview_url in base:
            return base
        
        # Add punctuation if needed
        if not base.endswith(('.', '!', '?')):
            base += '.'
        
        enhanced = f"{base} Preview: {preview_url}"
        
        LOGGER.log("PreviewURLGuardian", "phase2_embedded", TaskStatus.FALLBACK_USED,
                   "Embedded preview URL")
        
        return enhanced
    
    def phase3_verify_saved(self, name: str, expected_url: str, results_worksheet) -> bool:
        """Phase 3: Verify URL saved"""
        LOGGER.log("PreviewURLGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Verifying URL for {name}")
        
        time.sleep(2)
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase3 URL verify",
                None
            )
            
            for idx, row in enumerate(results_data):
                row_name = str(row.get("Restaurant Name", "")).strip()
                
                if re.sub(r'[^a-z0-9]', '', row_name.lower()) == re.sub(r'[^a-z0-9]', '', name.lower()):
                    preview_url_col = str(row.get("Preview URL", "")).strip()
                    ice_breaker = str(row.get("Ice_Breaker", "")).strip()
                    
                    url_in_column = expected_url in preview_url_col
                    url_in_icebreaker = expected_url in ice_breaker
                    
                    if url_in_column and url_in_icebreaker:
                        LOGGER.log("PreviewURLGuardian", "phase3_verified", TaskStatus.SUCCESS,
                                   "URL verified in both locations")
                        return True
                    else:
                        row_num = idx + 2
                        try:
                            if not url_in_column:
                                results_worksheet.update_cell(row_num, 5, expected_url)
                            
                            if not url_in_icebreaker:
                                fixed_ice = self.phase2_embed_in_icebreaker(ice_breaker, expected_url)
                                results_worksheet.update_cell(row_num, 16, fixed_ice)
                            
                            LOGGER.log("PreviewURLGuardian", "phase3_fixed", TaskStatus.FALLBACK_USED,
                                       "Fixed URL placement")
                            return True
                        except Exception as e:
                            LOGGER.log("PreviewURLGuardian", "phase3_fix_failed", TaskStatus.CATASTROPHIC,
                                       f"Failed to fix: {e}")
                            return False
            
            return False
            
        except Exception as e:
            LOGGER.log("PreviewURLGuardian", "phase3_error", TaskStatus.FAILED,
                       f"Verification failed: {e}")
            return False

# ============================================================================
# 📋 DATA INTEGRITY GUARDIAN
# ============================================================================
class DataIntegrityGuardian:
    """Ensures all data is in correct columns"""
    
    def validate_row_structure(self, lead_data: LeadData) -> Tuple[bool, List[str]]:
        """Validate data structure"""
        issues = []
        
        if not lead_data.restaurant_name:
            issues.append("Missing restaurant name")
        
        if not lead_data.flaw_analysis or len(lead_data.flaw_analysis) < 20:
            issues.append("Invalid flaw analysis")
        
        if not lead_data.preview_url or "lead-gen-engine" not in lead_data.preview_url:
            issues.append("Invalid preview URL")
        
        if not lead_data.ice_breaker or len(lead_data.ice_breaker) < 20:
            issues.append("Invalid ice breaker")
        
        if lead_data.preview_url not in lead_data.ice_breaker:
            issues.append("Preview URL not in ice breaker")
        
        if issues:
            LOGGER.log("DataIntegrityGuardian", "validation_failed", TaskStatus.FAILED,
                       f"Issues: {', '.join(issues)}")
            return False, issues
        
        LOGGER.log("DataIntegrityGuardian", "validation_passed", TaskStatus.SUCCESS,
                   "Data structure validated")
        return True, []
    
    def verify_saved_columns(self, name: str, expected_data: LeadData, 
                            results_worksheet) -> bool:
        """Verify columns"""
        time.sleep(2)
        
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Column integrity verify",
                None
            )
            
            for idx, row in enumerate(results_data):
                if re.sub(r'[^a-z0-9]', '', str(row.get("Restaurant Name", "")).lower()) == \
                   re.sub(r'[^a-z0-9]', '', name.lower()):
                    
                    checks = {
                        "Restaurant Name": row.get("Restaurant Name") == expected_data.restaurant_name,
                        "Preview URL": row.get("Preview URL") == expected_data.preview_url,
                        "Phone Number": row.get("Phone Number") == expected_data.phone,
                        "Ice Breaker": expected_data.preview_url in str(row.get("Ice_Breaker", ""))
                    }
                    
                    if all(checks.values()):
                        LOGGER.log("DataIntegrityGuardian", "columns_verified", TaskStatus.SUCCESS,
                                   "All columns correct")
                        return True
                    else:
                        failed = [k for k, v in checks.items() if not v]
                        LOGGER.log("DataIntegrityGuardian", "column_mismatch", TaskStatus.FAILED,
                                   f"Issues: {', '.join(failed)}")
                        return False
            
            return False
            
        except Exception as e:
            LOGGER.log("DataIntegrityGuardian", "verification_error", TaskStatus.FAILED,
                       f"Verification failed: {e}")
            return False

# ============================================================================
# 🏥 SYSTEM HEALTH GUARDIAN
# ============================================================================
class SystemHealthGuardian:
    """Monitors overall system health"""
    
    def __init__(self):
        self.health_data = {
            "last_check": None,
            "ollama_status": False,
            "sheets_status": False,
            "disk_space_mb": 0,
            "memory_usage_pct": 0
        }
    
    def check_health(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        LOGGER.log("SystemHealthGuardian", "health_check_start", TaskStatus.SUCCESS,
                   "Running system health check")
        
        try:
            ask_ollama("OK", max_tokens=5)
            self.health_data["ollama_status"] = True
        except:
            self.health_data["ollama_status"] = False
            LOGGER.log("SystemHealthGuardian", "ollama_down", TaskStatus.CATASTROPHIC,
                       "Ollama is not responding!")
        
        try:
            import shutil
            total, used, free = shutil.disk_usage("/")
            self.health_data["disk_space_mb"] = free // (1024 * 1024)
            
            if free < 1000 * 1024 * 1024:
                LOGGER.log("SystemHealthGuardian", "low_disk", TaskStatus.FAILED,
                           f"Low disk space: {free // (1024 * 1024)}MB")
        except:
            pass
        
        try:
            import psutil
            self.health_data["memory_usage_pct"] = psutil.virtual_memory().percent
            
            if self.health_data["memory_usage_pct"] > 90:
                LOGGER.log("SystemHealthGuardian", "high_memory", TaskStatus.FAILED,
                           f"High memory usage: {self.health_data['memory_usage_pct']}%")
        except:
            pass
        
        self.health_data["last_check"] = datetime.now().isoformat()
        
        with open(HEALTH_CHECK_FILE, 'w') as f:
            json.dump(self.health_data, f, indent=2)
        
        return self.health_data

# ============================================================================
# 📊 RATE LIMIT GUARDIAN (GPT-5: LOWERED TO 30)
# ============================================================================
class RateLimitGuardian:
    """Prevents rate limit violations"""
    
    def __init__(self):
        self.request_log = []
        self.max_requests_per_minute = 30  # GPT-5 suggestion
    
    def can_make_request(self) -> bool:
        """Check if request is allowed"""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        self.request_log = [t for t in self.request_log if t > one_minute_ago]
        
        if len(self.request_log) >= self.max_requests_per_minute:
            LOGGER.log("RateLimitGuardian", "limit_reached", TaskStatus.BLOCKED,
                       "Rate limit reached - waiting")
            return False
        
        return True
    
    def wait_if_needed(self):
        """Wait until request can be made"""
        while not self.can_make_request():
            time.sleep(2)
        
        self.request_log.append(datetime.now())

# ============================================================================
# 🔄 BACKUP GUARDIAN
# ============================================================================
class BackupGuardian:
    """Ensures data is never lost"""
    
    def __init__(self):
        self.backup_dir = "lead_backups"
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def backup_lead_data(self, lead_data: LeadData):
        """Backup lead data locally"""
        try:
            backup_file = os.path.join(
                self.backup_dir,
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{slug_ascii(lead_data.restaurant_name)}.json"
            )
            
            with open(backup_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "restaurant_name": lead_data.restaurant_name,
                        "phone": lead_data.phone,
                        "website_url": lead_data.website_url,
                        "flaw_analysis": lead_data.flaw_analysis,
                        "preview_url": lead_data.preview_url,
                        "ice_breaker": lead_data.ice_breaker
                    }
                }, f, indent=2)
            
            LOGGER.log("BackupGuardian", "backup_saved", TaskStatus.SUCCESS,
                       f"Backed up: {backup_file}")
            
        except Exception as e:
            LOGGER.log("BackupGuardian", "backup_failed", TaskStatus.FAILED,
                       f"Backup failed: {e}")

# ============================================================================
# 📊 PROGRESS TRACKER
# ============================================================================
class ProgressTracker:
    """Real-time progress tracking"""
    
    def __init__(self, daily_goal: int):
        self.daily_goal = daily_goal
        self.session_start = datetime.now()
        self.processed = 0
        self.successful = 0
        self.failed = 0
        self.duplicates_blocked = 0
    
    def update(self, success: bool, duplicate: bool = False):
        """Update progress"""
        self.processed += 1
        if duplicate:
            self.duplicates_blocked += 1
        elif success:
            self.successful += 1
        else:
            self.failed += 1
        
        self._display_progress()
    
    def _display_progress(self):
        """Display progress"""
        elapsed = (datetime.now() - self.session_start).total_seconds()
        elapsed_min = elapsed / 60
        
        progress_pct = (self.successful / self.daily_goal * 100) if self.daily_goal > 0 else 0
        remaining = self.daily_goal - self.successful
        
        if self.successful > 0:
            avg_time_per_lead = elapsed / self.successful
            eta_seconds = remaining * avg_time_per_lead
            eta_min = eta_seconds / 60
        else:
            eta_min = 0
        
        bar_length = 30
        filled = int(bar_length * progress_pct / 100)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\n{'='*70}")
        print(f"📊 PROGRESS TRACKER")
        print(f"{'='*70}")
        print(f"🎯 Goal: {self.successful}/{self.daily_goal} leads ({progress_pct:.1f}%)")
        print(f"[{bar}] {progress_pct:.1f}%")
        print(f"")
        print(f"✅ Successful: {self.successful}")
        print(f"❌ Failed: {self.failed}")
        print(f"🚫 Duplicates Blocked: {self.duplicates_blocked}")
        print(f"📈 Total Processed: {self.processed}")
        print(f"")
        print(f"⏱️  Elapsed: {elapsed_min:.1f} min")
        print(f"⏳ ETA: {eta_min:.1f} min")
        print(f"{'='*70}\n")

# ============================================================================
# 😴 REST MANAGER
# ============================================================================
class RestManager:
    """Manages rest periods"""
    
    def __init__(self, rest_after: int, rest_duration: int):
        self.rest_after = rest_after
        self.rest_duration = rest_duration
        self.leads_since_rest = 0
    
    def should_rest(self) -> bool:
        return self.leads_since_rest >= self.rest_after
    
    def take_rest(self):
        LOGGER.log("RestManager", "rest_start", TaskStatus.SUCCESS,
                   f"Taking {self.rest_duration}s rest")
        
        print(f"\n{'='*70}")
        print(f"😴 REST PERIOD")
        print(f"{'='*70}")
        print(f"✅ Completed {self.leads_since_rest} leads")
        print(f"⏰ Resting for {self.rest_duration / 60:.1f} minutes")
        print(f"{'='*70}\n")
        
        time.sleep(self.rest_duration)
        
        self.leads_since_rest = 0
        
        print(f"\n{'='*70}")
        print(f"🚀 RESUMING OPERATIONS")
        print(f"{'='*70}\n")
    
    def increment(self):
        self.leads_since_rest += 1

# ============================================================================
# 🎯 MASTER ORCHESTRATOR (WITH FULL ANALYSIS FLOW)
# ============================================================================
class MasterOrchestrator:
    """Coordinates all guardians"""
    
    def __init__(self, daily_goal: int, leads_worksheet, results_worksheet):
        self.duplicate_guardian = DuplicateGuardian()
        self.phone_guardian = PhoneSyncGuardian()
        self.preview_guardian = PreviewURLGuardian()
        self.data_guardian = DataIntegrityGuardian()
        self.health_guardian = SystemHealthGuardian()
        self.rate_limit_guardian = RateLimitGuardian()
        self.backup_guardian = BackupGuardian()
        self.progress_tracker = ProgressTracker(daily_goal)
        self.rest_manager = RestManager(REST_AFTER_LEADS, REST_DURATION)
        
        self.leads_worksheet = leads_worksheet
        self.results_worksheet = results_worksheet
        
        self.phone_guardian.phase1_build_map(leads_worksheet)
        self.health_guardian.check_health()
    
    def process_lead_fully_supervised(self, lead: Dict, lead_row_index: int) -> bool:
        """Process a lead with COMPLETE supervision + FULL ANALYSIS"""
        
        restaurant_name = str(lead.get("Restaurant Name", "")).strip()
        phone_raw = str(lead.get("Phone Number", "")).strip()
        target_url = lead.get("Website URL", "").strip()
        
        LOGGER.log("MasterOrchestrator", "lead_start", TaskStatus.SUCCESS,
                   f"🎯 STARTING: {restaurant_name}")
        
        # Rate limit check
        self.rate_limit_guardian.wait_if_needed()
        
        # Duplicate check Phase 1
        is_dup, reason = self.duplicate_guardian.phase1_check_before(
            restaurant_name, phone_raw, self.results_worksheet
        )
        
        if is_dup:
            safe_sheet_write(
                lambda: self.leads_worksheet.update_cell(lead_row_index, 6, "Complete - Duplicate"),
                "Mark duplicate"
            )
            self.progress_tracker.update(success=False, duplicate=True)
            return False
        
        # Get correct phone
        correct_phone = self.phone_guardian.phase2_get_correct_phone(restaurant_name, phone_raw)
        
        # Generate preview URL
        preview_url = self.preview_guardian.phase1_generate(restaurant_name)
        
        # Mark processing
        try:
            safe_sheet_write(
                lambda: self.leads_worksheet.update_cell(lead_row_index, 6, 
                                                        f"Processing... {datetime.now().strftime('%H:%M:%S')}"),
                "Mark processing"
            )
        except:
            pass
        
        # ═══════════════════════════════════════════════════════════════
        # FULL ANALYSIS FLOW (GPT-5 VERSION)
        # ═══════════════════════════════════════════════════════════════
        
        if not target_url or target_url.lower() in ["no website found", "", "n/a"]:
            # NO WEBSITE PATH
            LOGGER.log("MasterOrchestrator", "no_website", TaskStatus.SUCCESS,
                       "Taking no-website path")
            
            flaw_analysis = f"No website found for {restaurant_name}. This business needs a complete web presence to capture online customers."
            builder_prompt = "Create modern site: mobile-first, contact info, menu/services, SEO"
            ice_breaker = generate_fallback_ice_breaker(restaurant_name, preview_url)
            
        else:
            # WEBSITE EXISTS — FULL ANALYSIS
            LOGGER.log("MasterOrchestrator", "scraping_start", TaskStatus.SUCCESS,
                       f"Scraping {target_url}")
            
            try:
                # Scrape with Playwright
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    try:
                        page.goto(target_url, timeout=60000)
                        body_html = page.locator("body").inner_html()
                        LOGGER.log("MasterOrchestrator", "scraping_success", TaskStatus.SUCCESS,
                                   f"Scraped {len(body_html)} chars")
                    finally:
                        browser.close()
                
                cleaned_html = clean_html_aggressive(body_html)
                
            except Exception as e:
                LOGGER.log("MasterOrchestrator", "scraping_failed", TaskStatus.FAILED,
                           f"Scraping failed: {e}")
                cleaned_html = ""
            
            # AI Analysis
            try:
                if cleaned_html:
                    prompt = f"""Analyze the website for "{restaurant_name}" and provide:

1. KEY INFORMATION (3-4 bullet points):
   - What the business does
   - Contact info found/missing
   - Main issues

2. FIX CHECKLIST (5-7 actionable items):
   - Missing contact details, broken UX, SEO issues

3. ICE BREAKER (1-2 sentences, URGENT TONE):
   - Write as if you already know them.
   - Imply urgent risk (losing customers, competitors winning).
   - Reference something SPECIFIC from their site.
   - Make it personal and time-sensitive.

WEBSITE DATA:
{cleaned_html}
"""
                    
                    LOGGER.log("MasterOrchestrator", "ollama_start", TaskStatus.SUCCESS,
                               "Calling Ollama for analysis")
                    
                    full_response = ask_ollama(prompt, max_tokens=900, temperature=0.3)
                    
                    # Extract flaw analysis (everything before ice breaker)
                    flaw_analysis = full_response.strip()
                    
                    # Extract ice breaker
                    ice_breaker_extracted = extract_ice_breaker(full_response)
                    
                    if ice_breaker_extracted:
                        ice_breaker = ice_breaker_extracted
                        LOGGER.log("MasterOrchestrator", "ice_breaker_extracted", TaskStatus.SUCCESS,
                                   "Extracted ice breaker from AI")
                        
                        # Remove ice breaker from flaw analysis
                        if ICE_BREAKER_HEADER_RE.search(flaw_analysis):
                            match = ICE_BREAKER_HEADER_RE.search(flaw_analysis)
                            flaw_analysis = flaw_analysis[:match.start()].strip()
                    else:
                        ice_breaker = generate_site_ice_breaker(restaurant_name, cleaned_html, preview_url)
                        LOGGER.log("MasterOrchestrator", "ice_breaker_fallback", TaskStatus.FALLBACK_USED,
                                   "Using fallback ice breaker")
                    
                    builder_prompt = "Fix critical issues: contact info, mobile UX, SEO, speed"
                    
                else:
                    # Scrape failed but website exists
                    LOGGER.log("MasterOrchestrator", "no_html", TaskStatus.FAILED,
                               "No HTML retrieved - using soft fallback")
                    
                    flaw_analysis = f"Could not retrieve site content for {restaurant_name}. Site may have technical issues preventing access."
                    ice_breaker = generate_site_ice_breaker(restaurant_name, f"TITLE: {restaurant_name}", preview_url)
                    builder_prompt = "Fix site accessibility, add backup contact methods"
                    
            except Exception as e:
                LOGGER.log("MasterOrchestrator", "ollama_failed", TaskStatus.FAILED,
                           f"Ollama analysis failed: {e}")
                
                flaw_analysis = f"Analysis unavailable for {restaurant_name} due to temporary AI error. Manual review recommended."
                ice_breaker = generate_site_ice_breaker(restaurant_name, f"TITLE: {restaurant_name}", preview_url)
                builder_prompt = "Manual review needed - apply standard best practices"
        
        # Ensure preview URL in ice breaker
        ice_breaker = self.preview_guardian.phase2_embed_in_icebreaker(ice_breaker, preview_url)
        
        # Create lead data
        lead_data = LeadData(
            restaurant_name=restaurant_name,
            phone=correct_phone,
            website_url=target_url,
            flaw_analysis=flaw_analysis,
            builder_prompt=builder_prompt,
            preview_url=preview_url,
            ice_breaker=ice_breaker,
            row_index=lead_row_index
        )
        
        # Validate
        valid, issues = self.data_guardian.validate_row_structure(lead_data)
        if not valid:
            self.progress_tracker.update(success=False)
            return False
        
        # Backup
        self.backup_guardian.backup_lead_data(lead_data)
        
        # Duplicate check Phase 2
        is_dup, reason = self.duplicate_guardian.phase2_check_during(
            restaurant_name, correct_phone, self.results_worksheet
        )
        
        if is_dup:
            safe_sheet_write(
                lambda: self.leads_worksheet.update_cell(lead_row_index, 6, "Complete - Duplicate"),
                "Mark duplicate"
            )
            self.progress_tracker.update(success=False, duplicate=True)
            return False
        
        # Save
        try:
            safe_sheet_write(
                lambda: self.results_worksheet.append_row(lead_data.to_sheet_row()),
                "Save lead data"
            )
        except Exception as e:
            LOGGER.log("MasterOrchestrator", "save_failed", TaskStatus.CATASTROPHIC,
                       f"Save failed: {e}")
            self.progress_tracker.update(success=False)
            return False
        
        # Verify everything
        is_single, status = self.duplicate_guardian.phase3_verify_after(
            restaurant_name, correct_phone, self.results_worksheet
        )
        
        phone_synced = self.phone_guardian.phase3_verify_sync(
            restaurant_name, correct_phone, self.results_worksheet
        )
        
        url_verified = self.preview_guardian.phase3_verify_saved(
            restaurant_name, preview_url, self.results_worksheet
        )
        
        columns_ok = self.data_guardian.verify_saved_columns(
            restaurant_name, lead_data, self.results_worksheet
        )
        
        all_verified = is_single and phone_synced and url_verified and columns_ok
        
        if all_verified:
            LOGGER.log("MasterOrchestrator", "lead_complete", TaskStatus.SUCCESS,
                       f"✅ FULLY VERIFIED: {restaurant_name}")
            
            safe_sheet_write(
                lambda: self.leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                "Mark complete"
            )
            
            self.progress_tracker.update(success=True)
            self.rest_manager.increment()
            return True
        else:
            self.progress_tracker.update(success=False)
            return False

# ============================================================================
# MAIN
# ============================================================================
def main():
    """Main loop"""
    
    verify_ollama()
    
    print("\n" + "="*70)
    print("🚀 ULTRA-SUPERVISED LEAD PROCESSOR")
    print("="*70)
    print(f"💎 Model: {OLLAMA_MODEL}")
    print(f"🛡️  9 Guardian Systems Active:")
    print(f"   1. Duplicate Guardian (3-phase)")
    print(f"   2. Phone Sync Guardian (3-phase)")
    print(f"   3. Preview URL Guardian (3-phase)")
    print(f"   4. Data Integrity Guardian")
    print(f"   5. System Health Guardian")
    print(f"   6. Rate Limit Guardian (30/min)")
    print(f"   7. Backup Guardian")
    print(f"   8. Progress Tracker")
    print(f"   9. Rest Manager")
    print(f"📊 Daily Goal: {MAX_LEADS_PER_DAY} leads")
    print(f"🔥 FULL ANALYSIS: Playwright + Ollama")
    print(f"💬 SOLID ICE BREAKERS: Personalized + Urgent")
    print(f"💰 Cost: ₹0 FOREVER!")
    print("="*70 + "\n")
    
    orchestrator = MasterOrchestrator(MAX_LEADS_PER_DAY, leads_worksheet, results_worksheet)
    
    while True:
        try:
            if orchestrator.rest_manager.should_rest():
                orchestrator.rest_manager.take_rest()
                orchestrator.health_guardian.check_health()
            
            all_leads = safe_sheet_read(
                lambda: leads_worksheet.get_all_records(),
                "Fetch leads",
                None
            )
            
            processed_this_cycle = False
            
            for idx, lead in enumerate(all_leads):
                status = str(lead.get("Status", "")).strip().lower()
                
                if status == "pending":
                    lead_row_index = idx + 2
                    
                    success = orchestrator.process_lead_fully_supervised(
                        lead, lead_row_index
                    )
                    
                    processed_this_cycle = True
                    
                    if success:
                        delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                        print(f"⏸️  Waiting {delay}s...\n")
                        time.sleep(delay)
                    
                    break
            
            if not processed_this_cycle:
                print("ℹ️  No pending leads. Waiting...")
                time.sleep(RETRY_DELAY_SECONDS)
                
        except KeyboardInterrupt:
            print("\n⛔ Stopped by user")
            break
        except Exception as e:
            LOGGER.log("MainLoop", "error", TaskStatus.CATASTROPHIC, f"Error: {e}")
            time.sleep(RETRY_DELAY_SECONDS)

if __name__ == "__main__":
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        leads_worksheet = spreadsheet.worksheet("LEADS")
        results_worksheet = spreadsheet.worksheet("RESULTS")
        print("✅ Connected to Google Sheets\n")
    except Exception as e:
        print(f"❌ FATAL: {e}")
        exit(1)
    
    main()
