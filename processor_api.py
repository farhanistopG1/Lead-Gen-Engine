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
import hashlib

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
REST_AFTER_LEADS = 10  # Take rest after every 10 leads
REST_DURATION = 300  # 5 minutes rest

# Files
TRACKING_FILE = "daily_processing_log.json"
CACHE_FILE = "sheets_cache.json"
SUPERVISOR_LOG_FILE = "supervisor_decisions.jsonl"
DUPLICATE_REGISTRY_FILE = "duplicate_registry.json"
PHONE_SYNC_LOG_FILE = "phone_sync_log.json"
PROGRESS_FILE = "progress_tracker.json"

# Limits
CACHE_DURATION = 300
MAX_RETRIES = 5
BASE_BACKOFF = 10
MAX_HTML_LENGTH = 8000

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
            self.restaurant_name,     # 1
            self.flaw_analysis,       # 2
            self.builder_prompt,      # 3
            "",                       # 4. Outreach Status
            self.preview_url,         # 5. Preview URL
            self.phone,               # 6. Phone Number
            "",                       # 7. Message ID
            "",                       # 8. Last_Message_Sent
            "",                       # 9. Last_Message_Content
            "",                       # 10. Last_Reply_Received
            "",                       # 11. Last_Reply_Content
            "",                       # 12. Follow_Up_Count
            "",                       # 13. Auto_Acknowledge_Sent
            "",                       # 14. VAPI_Call_Scheduled
            "",                       # 15. Call_Scheduled_At
            self.ice_breaker,         # 16. Ice_Breaker
            ""                        # 17. Source_Row
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
        
        # Console output
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
        
        # File output
        try:
            with open(SUPERVISOR_LOG_FILE, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"⚠️  Log write failed: {e}")

LOGGER = SupervisorLogger()

# ============================================================================
# 🛡️ DUPLICATE GUARDIAN - 3-PHASE PROTECTION
# ============================================================================
class DuplicateGuardian:
    """
    Triple-layer duplicate prevention:
    - Phase 1: Before processing (registry check)
    - Phase 2: During processing (live check)
    - Phase 3: After processing (verification)
    """
    
    def __init__(self):
        self.registry = self._load_registry()
    
    def _load_registry(self) -> Dict:
        """Load persistent duplicate registry"""
        if os.path.exists(DUPLICATE_REGISTRY_FILE):
            try:
                with open(DUPLICATE_REGISTRY_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {"keys": {}, "last_updated": None}
    
    def _save_registry(self):
        """Save registry to disk"""
        self.registry["last_updated"] = datetime.now().isoformat()
        with open(DUPLICATE_REGISTRY_FILE, 'w') as f:
            json.dump(self.registry, f, indent=2)
    
    def _create_duplicate_key(self, name: str, phone: str) -> str:
        """Create unique key for duplicate detection"""
        name_norm = re.sub(r'[^a-z0-9]', '', name.lower())
        phone_norm = ''.join(filter(str.isdigit, phone))[-10:] if phone else ""
        
        # Use phone if available, else name
        if phone_norm and len(phone_norm) == 10:
            key = f"phone:{phone_norm}"
        elif name_norm:
            key = f"name:{name_norm}"
        else:
            key = None
        
        return key
    
    def _generate_fingerprint(self, name: str, phone: str) -> str:
        """Generate unique fingerprint"""
        content = f"{name.lower().strip()}:{phone.strip()}"
        return hashlib.md5(content.encode()).hexdigest()
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 1: BEFORE PROCESSING
    # ═══════════════════════════════════════════════════════════════
    def phase1_check_before(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """
        Phase 1: Check BEFORE processing starts
        Returns: (is_duplicate, reason)
        """
        LOGGER.log("DuplicateGuardian", "phase1_start", TaskStatus.SUCCESS,
                   f"Phase 1 check for {name}")
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            LOGGER.log("DuplicateGuardian", "phase1_no_key", TaskStatus.SUCCESS,
                       "No valid key - allowing")
            return False, "no_key"
        
        # Check 1: Registry (fast local check)
        if dup_key in self.registry["keys"]:
            LOGGER.log("DuplicateGuardian", "phase1_registry_hit", TaskStatus.BLOCKED,
                       f"Found in registry: {dup_key}")
            return True, "registry"
        
        # Check 2: Live sheet check
        try:
            results_data = safe_sheet_read(
                lambda: results_worksheet.get_all_records(),
                "Phase1 sheet check",
                None  # No cache for duplicate checks
            )
            
            for row in results_data:
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    LOGGER.log("DuplicateGuardian", "phase1_sheet_hit", TaskStatus.BLOCKED,
                               f"Found in sheet: {existing_name}")
                    # Add to registry
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
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 2: DURING PROCESSING
    # ═══════════════════════════════════════════════════════════════
    def phase2_check_during(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """
        Phase 2: Check DURING processing (before save)
        Returns: (is_duplicate, reason)
        """
        LOGGER.log("DuplicateGuardian", "phase2_start", TaskStatus.SUCCESS,
                   f"Phase 2 check for {name}")
        
        # Wait a moment for any concurrent operations
        time.sleep(2)
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            return False, "no_key"
        
        # Fresh sheet check (no cache)
        try:
            results_data = results_worksheet.get_all_records()
            
            for row in results_data:
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    LOGGER.log("DuplicateGuardian", "phase2_duplicate", TaskStatus.BLOCKED,
                               f"Duplicate detected during processing: {existing_name}")
                    return True, "concurrent"
            
            LOGGER.log("DuplicateGuardian", "phase2_passed", TaskStatus.SUCCESS,
                       f"Phase 2 passed for {name}")
            return False, "passed"
            
        except Exception as e:
            LOGGER.log("DuplicateGuardian", "phase2_error", TaskStatus.FAILED,
                       f"Phase 2 check failed: {e}")
            return False, "error"
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 3: AFTER PROCESSING
    # ═══════════════════════════════════════════════════════════════
    def phase3_verify_after(self, name: str, phone: str, results_worksheet) -> Tuple[bool, str]:
        """
        Phase 3: Verify AFTER save
        Returns: (is_single, status)
        """
        LOGGER.log("DuplicateGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Phase 3 verification for {name}")
        
        # Wait for write to settle
        time.sleep(3)
        
        dup_key = self._create_duplicate_key(name, phone)
        if not dup_key:
            return True, "no_key"
        
        try:
            results_data = results_worksheet.get_all_records()
            
            matches = []
            for idx, row in enumerate(results_data):
                existing_name = str(row.get("Restaurant Name", "")).strip()
                existing_phone = str(row.get("Phone Number", "")).strip()
                existing_key = self._create_duplicate_key(existing_name, existing_phone)
                
                if existing_key and dup_key == existing_key:
                    matches.append((idx + 2, existing_name, existing_phone))
            
            if len(matches) == 0:
                LOGGER.log("DuplicateGuardian", "phase3_missing", TaskStatus.CATASTROPHIC,
                           f"Entry not found after save!")
                return False, "missing"
            
            elif len(matches) == 1:
                LOGGER.log("DuplicateGuardian", "phase3_success", TaskStatus.SUCCESS,
                           f"Verified single entry for {name}")
                # Add to registry
                self.registry["keys"][dup_key] = {
                    "name": name,
                    "phone": phone,
                    "added": datetime.now().isoformat()
                }
                self._save_registry()
                return True, "verified"
            
            else:
                LOGGER.log("DuplicateGuardian", "phase3_duplicates_found", TaskStatus.CATASTROPHIC,
                           f"Found {len(matches)} duplicates for {name}!")
                
                # Delete all but first
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
# 📞 PHONE SYNC GUARDIAN - 3-PHASE SYNC
# ============================================================================
class PhoneSyncGuardian:
    """
    Ensures phone numbers are always in sync:
    - Phase 1: Before processing (validate source)
    - Phase 2: During processing (embed correct phone)
    - Phase 3: After processing (verify sync)
    """
    
    def __init__(self):
        self.phone_map = {}
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for matching"""
        return re.sub(r'[^a-z0-9]', '', name.lower())
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone"""
        digits = ''.join(filter(str.isdigit, str(phone)))
        return digits[-10:] if len(digits) >= 10 else digits
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 1: BUILD PHONE MAP
    # ═══════════════════════════════════════════════════════════════
    def phase1_build_map(self, leads_worksheet):
        """Phase 1: Build authoritative phone map from LEADS"""
        LOGGER.log("PhoneSyncGuardian", "phase1_start", TaskStatus.SUCCESS,
                   "Building phone map from LEADS")
        
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
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 2: GET CORRECT PHONE
    # ═══════════════════════════════════════════════════════════════
    def phase2_get_correct_phone(self, name: str, provided_phone: str) -> str:
        """Phase 2: Get authoritative phone for this lead"""
        name_norm = self._normalize_name(name)
        
        if name_norm in self.phone_map:
            correct_phone = self.phone_map[name_norm]
            
            if correct_phone != provided_phone:
                LOGGER.log("PhoneSyncGuardian", "phase2_correction", TaskStatus.FALLBACK_USED,
                           f"Correcting phone for {name}: {provided_phone} → {correct_phone}")
            else:
                LOGGER.log("PhoneSyncGuardian", "phase2_match", TaskStatus.SUCCESS,
                           f"Phone matches for {name}")
            
            return correct_phone
        else:
            LOGGER.log("PhoneSyncGuardian", "phase2_not_found", TaskStatus.FAILED,
                       f"Name not found in map: {name}")
            return provided_phone if provided_phone else "No Number"
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 3: VERIFY AFTER SAVE
    # ═══════════════════════════════════════════════════════════════
    def phase3_verify_sync(self, name: str, expected_phone: str, results_worksheet) -> bool:
        """Phase 3: Verify phone is correctly saved"""
        LOGGER.log("PhoneSyncGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Verifying phone for {name}")
        
        time.sleep(2)
        
        try:
            results_data = results_worksheet.get_all_records()
            
            for idx, row in enumerate(results_data):
                row_name = str(row.get("Restaurant Name", "")).strip()
                
                if self._normalize_name(row_name) == self._normalize_name(name):
                    saved_phone = str(row.get("Phone Number", "")).strip()
                    
                    if saved_phone == expected_phone:
                        LOGGER.log("PhoneSyncGuardian", "phase3_verified", TaskStatus.SUCCESS,
                                   f"Phone verified for {name}: {saved_phone}")
                        return True
                    else:
                        LOGGER.log("PhoneSyncGuardian", "phase3_mismatch", TaskStatus.FAILED,
                                   f"Phone mismatch for {name}: expected {expected_phone}, got {saved_phone}")
                        
                        # Fix it
                        row_num = idx + 2
                        try:
                            results_worksheet.update_cell(row_num, 6, expected_phone)
                            LOGGER.log("PhoneSyncGuardian", "phase3_fixed", TaskStatus.FALLBACK_USED,
                                       f"Fixed phone at row {row_num}")
                            return True
                        except Exception as e:
                            LOGGER.log("PhoneSyncGuardian", "phase3_fix_failed", TaskStatus.CATASTROPHIC,
                                       f"Failed to fix phone: {e}")
                            return False
            
            LOGGER.log("PhoneSyncGuardian", "phase3_not_found", TaskStatus.FAILED,
                       f"Entry not found for {name}")
            return False
            
        except Exception as e:
            LOGGER.log("PhoneSyncGuardian", "phase3_error", TaskStatus.FAILED,
                       f"Verification failed: {e}")
            return False

# ============================================================================
# 🔗 PREVIEW URL GUARDIAN - 3-PHASE VALIDATION
# ============================================================================
class PreviewURLGuardian:
    """
    Ensures preview URL is always generated and embedded:
    - Phase 1: Before processing (generate URL)
    - Phase 2: During processing (embed in ice breaker)
    - Phase 3: After processing (verify presence)
    """
    
    BASE_URL = "https://lead-gen-engine.vercel.app"
    
    def _generate_url(self, name: str) -> str:
        """Generate preview URL"""
        project_id = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        return f"{self.BASE_URL}/?client={project_id}"
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 1: GENERATE
    # ═══════════════════════════════════════════════════════════════
    def phase1_generate(self, name: str) -> str:
        """Phase 1: Generate preview URL"""
        url = self._generate_url(name)
        
        LOGGER.log("PreviewURLGuardian", "phase1_generated", TaskStatus.SUCCESS,
                   f"Generated URL for {name}: {url}")
        
        return url
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 2: EMBED IN ICE BREAKER
    # ═══════════════════════════════════════════════════════════════
    def phase2_embed_in_icebreaker(self, ice_breaker: str, preview_url: str) -> str:
        """Phase 2: Ensure URL is in ice breaker"""
        
        if preview_url in ice_breaker:
            LOGGER.log("PreviewURLGuardian", "phase2_already_present", TaskStatus.SUCCESS,
                       "Preview URL already in ice breaker")
            return ice_breaker
        
        # Add URL
        if not ice_breaker.endswith(('.', '!', '?')):
            ice_breaker += '.'
        
        enhanced = f"{ice_breaker} Preview: {preview_url}"
        
        LOGGER.log("PreviewURLGuardian", "phase2_embedded", TaskStatus.FALLBACK_USED,
                   "Embedded preview URL into ice breaker")
        
        return enhanced
    
    # ═══════════════════════════════════════════════════════════════
    # PHASE 3: VERIFY IN SAVED DATA
    # ═══════════════════════════════════════════════════════════════
    def phase3_verify_saved(self, name: str, expected_url: str, results_worksheet) -> bool:
        """Phase 3: Verify URL is in both columns"""
        LOGGER.log("PreviewURLGuardian", "phase3_start", TaskStatus.SUCCESS,
                   f"Verifying preview URL for {name}")
        
        time.sleep(2)
        
        try:
            results_data = results_worksheet.get_all_records()
            
            for idx, row in enumerate(results_data):
                row_name = str(row.get("Restaurant Name", "")).strip()
                
                if re.sub(r'[^a-z0-9]', '', row_name.lower()) == re.sub(r'[^a-z0-9]', '', name.lower()):
                    preview_url_col = str(row.get("Preview URL", "")).strip()
                    ice_breaker = str(row.get("Ice_Breaker", "")).strip()
                    
                    url_in_column = expected_url in preview_url_col
                    url_in_icebreaker = expected_url in ice_breaker
                    
                    if url_in_column and url_in_icebreaker:
                        LOGGER.log("PreviewURLGuardian", "phase3_verified", TaskStatus.SUCCESS,
                                   f"Preview URL verified in both locations for {name}")
                        return True
                    else:
                        LOGGER.log("PreviewURLGuardian", "phase3_missing", TaskStatus.FAILED,
                                   f"Preview URL missing - Column: {url_in_column}, Icebreaker: {url_in_icebreaker}")
                        
                        # Fix it
                        row_num = idx + 2
                        try:
                            if not url_in_column:
                                results_worksheet.update_cell(row_num, 5, expected_url)
                                LOGGER.log("PreviewURLGuardian", "phase3_fixed_column", TaskStatus.FALLBACK_USED,
                                           "Fixed Preview URL column")
                            
                            if not url_in_icebreaker:
                                fixed_ice = self.phase2_embed_in_icebreaker(ice_breaker, expected_url)
                                results_worksheet.update_cell(row_num, 16, fixed_ice)
                                LOGGER.log("PreviewURLGuardian", "phase3_fixed_icebreaker", TaskStatus.FALLBACK_USED,
                                           "Fixed ice breaker")
                            
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
# 📋 DATA INTEGRITY GUARDIAN - Column Validation
# ============================================================================
class DataIntegrityGuardian:
    """Ensures all data is in correct columns"""
    
    EXPECTED_COLUMNS = {
        1: "Restaurant Name",
        2: "Flaw Analysis", 
        3: "Builder Prompt",
        4: "Outreach Status",
        5: "Preview URL",
        6: "Phone Number",
        16: "Ice_Breaker"
    }
    
    def validate_row_structure(self, lead_data: LeadData) -> Tuple[bool, List[str]]:
        """Validate data structure before save"""
        issues = []
        
        # Check all required fields
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
                       f"Validation issues: {', '.join(issues)}")
            return False, issues
        
        LOGGER.log("DataIntegrityGuardian", "validation_passed", TaskStatus.SUCCESS,
                   f"Data structure validated for {lead_data.restaurant_name}")
        return True, []
    
    def verify_saved_columns(self, name: str, expected_data: LeadData, 
                            results_worksheet) -> bool:
        """Verify data saved to correct columns"""
        time.sleep(2)
        
        try:
            results_data = results_worksheet.get_all_records()
            
            for idx, row in enumerate(results_data):
                if re.sub(r'[^a-z0-9]', '', str(row.get("Restaurant Name", "")).lower()) == \
                   re.sub(r'[^a-z0-9]', '', name.lower()):
                    
                    # Verify each column
                    checks = {
                        "Restaurant Name": row.get("Restaurant Name") == expected_data.restaurant_name,
                        "Preview URL": row.get("Preview URL") == expected_data.preview_url,
                        "Phone Number": row.get("Phone Number") == expected_data.phone,
                        "Ice Breaker": expected_data.preview_url in str(row.get("Ice_Breaker", ""))
                    }
                    
                    all_correct = all(checks.values())
                    
                    if all_correct:
                        LOGGER.log("DataIntegrityGuardian", "columns_verified", TaskStatus.SUCCESS,
                                   f"All columns correct for {name}")
                        return True
                    else:
                        failed = [k for k, v in checks.items() if not v]
                        LOGGER.log("DataIntegrityGuardian", "column_mismatch", TaskStatus.FAILED,
                                   f"Column issues: {', '.join(failed)}")
                        return False
            
            return False
            
        except Exception as e:
            LOGGER.log("DataIntegrityGuardian", "verification_error", TaskStatus.FAILED,
                       f"Column verification failed: {e}")
            return False

# ============================================================================
# 📊 PROGRESS TRACKER
# ============================================================================
class ProgressTracker:
    """Real-time progress tracking with goals"""
    
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
        """Display beautiful progress"""
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
        
        # Progress bar
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
    """Manages rest periods for the system"""
    
    def __init__(self, rest_after: int, rest_duration: int):
        self.rest_after = rest_after
        self.rest_duration = rest_duration
        self.leads_since_rest = 0
    
    def should_rest(self) -> bool:
        """Check if system should rest"""
        return self.leads_since_rest >= self.rest_after
    
    def take_rest(self):
        """Take a rest period"""
        LOGGER.log("RestManager", "rest_start", TaskStatus.SUCCESS,
                   f"Taking {self.rest_duration}s rest after {self.leads_since_rest} leads")
        
        print(f"\n{'='*70}")
        print(f"😴 REST PERIOD")
        print(f"{'='*70}")
        print(f"✅ Completed {self.leads_since_rest} leads")
        print(f"⏰ Resting for {self.rest_duration / 60:.1f} minutes")
        print(f"🔋 System health check...")
        print(f"{'='*70}\n")
        
        time.sleep(self.rest_duration)
        
        self.leads_since_rest = 0
        
        LOGGER.log("RestManager", "rest_complete", TaskStatus.SUCCESS,
                   "Rest period complete - resuming operations")
        
        print(f"\n{'='*70}")
        print(f"🚀 RESUMING OPERATIONS")
        print(f"{'='*70}\n")
    
    def increment(self):
        """Increment lead counter"""
        self.leads_since_rest += 1

# ============================================================================
# 🎯 MASTER ORCHESTRATOR
# ============================================================================
class MasterOrchestrator:
    """Coordinates all guardians and manages the entire process"""
    
    def __init__(self, daily_goal: int):
        self.duplicate_guardian = DuplicateGuardian()
        self.phone_guardian = PhoneSyncGuardian()
        self.preview_guardian = PreviewURLGuardian()
        self.data_guardian = DataIntegrityGuardian()
        self.progress_tracker = ProgressTracker(daily_goal)
        self.rest_manager = RestManager(REST_AFTER_LEADS, REST_DURATION)
        
        # Initialize phone map
        self.phone_guardian.phase1_build_map(leads_worksheet)
    
    def process_lead_fully_supervised(self, lead: Dict, lead_row_index: int,
                                      results_worksheet) -> bool:
        """
        Process a lead with COMPLETE supervision.
        Returns True if processed successfully.
        """
        restaurant_name = str(lead.get("Restaurant Name", "")).strip()
        phone_raw = str(lead.get("Phone Number", "")).strip()
        target_url = lead.get("Website URL", "").strip()
        
        LOGGER.log("MasterOrchestrator", "lead_start", TaskStatus.SUCCESS,
                   f"🎯 STARTING: {restaurant_name}")
        
        # ═══════════════════════════════════════════════════════════════
        # PHASE 1: PRE-PROCESSING CHECKS
        # ═══════════════════════════════════════════════════════════════
        
        # Duplicate Check Phase 1
        is_dup, reason = self.duplicate_guardian.phase1_check_before(
            restaurant_name, phone_raw, results_worksheet
        )
        
        if is_dup:
            LOGGER.log("MasterOrchestrator", "duplicate_blocked", TaskStatus.BLOCKED,
                       f"Duplicate blocked at Phase 1: {reason}")
            
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete - Duplicate"),
                "Mark duplicate"
            )
            
            self.progress_tracker.update(success=False, duplicate=True)
            return False
        
        # Get correct phone
        correct_phone = self.phone_guardian.phase2_get_correct_phone(restaurant_name, phone_raw)
        
        # Generate preview URL
        preview_url = self.preview_guardian.phase1_generate(restaurant_name)
        
        # Mark as processing
        try:
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, 
                                                    f"Processing... {datetime.now().strftime('%H:%M:%S')}"),
                "Mark processing"
            )
        except:
            pass
        
        # ═══════════════════════════════════════════════════════════════
        # PHASE 2: PROCESSING
        # ═══════════════════════════════════════════════════════════════
        
        # Handle no website
        if not target_url or target_url.lower() in ["no website found", "", "n/a"]:
            flaw_analysis = "No website found. Cannot perform analysis."
            builder_prompt = "Create a modern, mobile-friendly website with contact info, menu, and SEO."
            ice_breaker = f"Hi, I noticed {restaurant_name} doesn't have a website—that's costing you 60%+ of customers. Preview: {preview_url} Can I show you how to launch in 24 hours?"
        else:
            # Scrape and analyze (simplified - use your existing code)
            try:
                # Use your ScrapingSupervisor and AIAnalysisSupervisor here
                flaw_analysis = f"Analysis for {restaurant_name}"  # Placeholder
                ice_breaker = f"Quick note about {restaurant_name}. Preview: {preview_url}"
                builder_prompt = "Template-based fixes"
            except:
                flaw_analysis = "Analysis failed"
                ice_breaker = f"Preview: {preview_url}"
                builder_prompt = "Template-based"
        
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
        
        # Validate data structure
        valid, issues = self.data_guardian.validate_row_structure(lead_data)
        if not valid:
            LOGGER.log("MasterOrchestrator", "validation_failed", TaskStatus.FAILED,
                       f"Data validation failed: {issues}")
            self.progress_tracker.update(success=False)
            return False
        
        # Duplicate check Phase 2 (before save)
        is_dup, reason = self.duplicate_guardian.phase2_check_during(
            restaurant_name, correct_phone, results_worksheet
        )
        
        if is_dup:
            LOGGER.log("MasterOrchestrator", "duplicate_blocked_phase2", TaskStatus.BLOCKED,
                       "Duplicate detected at Phase 2")
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete - Duplicate"),
                "Mark duplicate"
            )
            self.progress_tracker.update(success=False, duplicate=True)
            return False
        
        # ═══════════════════════════════════════════════════════════════
        # PHASE 3: SAVING
        # ═══════════════════════════════════════════════════════════════
        
        # Save to sheet
        try:
            safe_sheet_write(
                lambda: results_worksheet.append_row(lead_data.to_sheet_row()),
                "Save lead data"
            )
            
            LOGGER.log("MasterOrchestrator", "save_success", TaskStatus.SUCCESS,
                       f"Saved {restaurant_name}")
            
        except Exception as e:
            LOGGER.log("MasterOrchestrator", "save_failed", TaskStatus.CATASTROPHIC,
                       f"Save failed: {e}")
            self.progress_tracker.update(success=False)
            return False
        
        # ═══════════════════════════════════════════════════════════════
        # PHASE 4: POST-PROCESSING VERIFICATION
        # ═══════════════════════════════════════════════════════════════
        
        # Verify no duplicates created
        is_single, status = self.duplicate_guardian.phase3_verify_after(
            restaurant_name, correct_phone, results_worksheet
        )
        
        # Verify phone sync
        phone_synced = self.phone_guardian.phase3_verify_sync(
            restaurant_name, correct_phone, results_worksheet
        )
        
        # Verify preview URL
        url_verified = self.preview_guardian.phase3_verify_saved(
            restaurant_name, preview_url, results_worksheet
        )
        
        # Verify column integrity
        columns_ok = self.data_guardian.verify_saved_columns(
            restaurant_name, lead_data, results_worksheet
        )
        
        # Overall success
        all_verified = is_single and phone_synced and url_verified and columns_ok
        
        if all_verified:
            LOGGER.log("MasterOrchestrator", "lead_complete", TaskStatus.SUCCESS,
                       f"✅ FULLY VERIFIED: {restaurant_name}")
            
            safe_sheet_write(
                lambda: leads_worksheet.update_cell(lead_row_index, 6, "Complete"),
                "Mark complete"
            )
            
            self.progress_tracker.update(success=True)
            self.rest_manager.increment()
            return True
        else:
            LOGGER.log("MasterOrchestrator", "verification_issues", TaskStatus.FAILED,
                       f"Verification incomplete for {restaurant_name}")
            
            self.progress_tracker.update(success=False)
            return False

# ============================================================================
# [YOUR EXISTING HELPER FUNCTIONS]
# ============================================================================
# Add all your existing functions here:
# - ask_ollama()
# - verify_ollama()
# - SheetsCache
# - clean_html_aggressive()
# - safe_sheet_read()
# - safe_sheet_write()
# - etc.

# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================
def main():
    """Main processing loop with full supervision"""
    
    # Verify Ollama
    verify_ollama()
    
    print("\n" + "="*70)
    print("🚀 ULTRA-SUPERVISED LEAD PROCESSOR")
    print("="*70)
    print(f"💎 Model: {OLLAMA_MODEL}")
    print(f"🛡️  6 Guardian Systems Active:")
    print(f"   1. Duplicate Guardian (3-phase)")
    print(f"   2. Phone Sync Guardian (3-phase)")
    print(f"   3. Preview URL Guardian (3-phase)")
    print(f"   4. Data Integrity Guardian")
    print(f"   5. Progress Tracker")
    print(f"   6. Rest Manager")
    print(f"📊 Daily Goal: {MAX_LEADS_PER_DAY} leads")
    print(f"😴 Rest: Every {REST_AFTER_LEADS} leads for {REST_DURATION/60:.0f} min")
    print(f"💰 Cost: ₹0 FOREVER!")
    print("="*70 + "\n")
    
    # Initialize orchestrator
    orchestrator = MasterOrchestrator(MAX_LEADS_PER_DAY)
    
    while True:
        try:
            # Check if rest is needed
            if orchestrator.rest_manager.should_rest():
                orchestrator.rest_manager.take_rest()
            
            # Fetch leads
            all_leads = safe_sheet_read(
                lambda: leads_worksheet.get_all_records(),
                "Fetch leads",
                None
            )
            
            # Process pending leads
            processed_this_cycle = False
            
            for idx, lead in enumerate(all_leads):
                status = str(lead.get("Status", "")).strip().lower()
                
                if status == "pending":
                    lead_row_index = idx + 2
                    
                    success = orchestrator.process_lead_fully_supervised(
                        lead, lead_row_index, results_worksheet
                    )
                    
                    processed_this_cycle = True
                    
                    # Delay between leads
                    if success:
                        delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                        print(f"⏸️  Waiting {delay}s before next lead...\n")
                        time.sleep(delay)
                    
                    break  # Process one at a time
            
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
        print(f"❌ FATAL: {e}")
        exit(1)
    
    main()
