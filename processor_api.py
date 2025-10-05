#!/usr/bin/env python3
"""
Lead Processor - Indian Restaurant Outreach
Syncs 3 leads/day from LEADS → RESULTS sheet
Handles Indian phone formats and integrates with n8n workflows
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import re
from datetime import datetime
from typing import Tuple, Optional
import logging
import sys
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    CREDENTIALS_FILE = 'credentials.json'
    SPREADSHEET_NAME = 'Lead Generation'
    LEADS_WORKSHEET = 'LEADS'
    RESULTS_WORKSHEET = 'RESULTS'
    
    DAILY_LEAD_LIMIT = 3
    API_RATE_LIMIT = 2  # seconds between API calls
    
    # Lock cell to prevent race conditions with n8n
    LOCK_CELL = 'Z1'
    LOCK_TIMEOUT = 300  # 5 minutes

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging():
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"processor_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ============================================================================
# PHONE VALIDATION (Indian Numbers)
# ============================================================================

class IndianPhoneValidator:
    """Validates and normalizes Indian phone numbers"""
    
    INVALID_STRINGS = {'not found', 'n/a', 'na', '', 'none', 'null'}
    MOBILE_PREFIXES = {'6', '7', '8', '9'}  # Indian mobile prefixes
    
    @staticmethod
    def normalize(phone: str) -> str:
        """Extract 10-digit mobile number"""
        if not phone:
            return ''
        
        # Remove all non-digits
        digits = ''.join(filter(str.isdigit, str(phone)))
        
        # Remove country code if present
        if digits.startswith('91') and len(digits) == 12:
            digits = digits[2:]
        
        # Remove leading 0
        if digits.startswith('0'):
            digits = digits[1:]
        
        # Return last 10 digits
        return digits[-10:] if len(digits) >= 10 else digits
    
    @staticmethod
    def validate(phone: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validate Indian phone number
        Returns: (is_valid, normalized_10_digit, error_message)
        """
        if not phone:
            return False, None, "Empty"
        
        phone_lower = str(phone).strip().lower()
        
        # Check for invalid literal values
        if phone_lower in IndianPhoneValidator.INVALID_STRINGS:
            return False, None, f"Literal: '{phone}'"
        
        # Normalize
        normalized = IndianPhoneValidator.normalize(phone)
        
        # Must be exactly 10 digits
        if len(normalized) != 10:
            return False, None, f"Length: {len(normalized)}"
        
        # Must start with mobile prefix
        if normalized[0] not in IndianPhoneValidator.MOBILE_PREFIXES:
            return False, None, f"Landline/Invalid: {normalized[0]}"
        
        return True, normalized, None

# ============================================================================
# LOCK MECHANISM
# ============================================================================

class SheetLock:
    """Prevents conflicts with n8n workflows"""
    
    def __init__(self, sheet, lock_cell: str, timeout: int):
        self.sheet = sheet
        self.lock_cell = lock_cell
        self.timeout = timeout
        self.lock_id = None
    
    def acquire(self) -> bool:
        """Acquire lock with retries"""
        try:
            for attempt in range(3):
                current = self.sheet.acell(self.lock_cell).value
                
                if not current:
                    lock_free = True
                else:
                    try:
                        lock_time = datetime.fromisoformat(current)
                        lock_free = (datetime.now() - lock_time).total_seconds() > self.timeout
                    except:
                        lock_free = True
                
                if lock_free:
                    self.lock_id = datetime.now().isoformat()
                    self.sheet.update_acell(self.lock_cell, self.lock_id)
                    time.sleep(2)
                    
                    if self.sheet.acell(self.lock_cell).value == self.lock_id:
                        logger.info(f"✓ Lock acquired")
                        return True
                
                logger.warning(f"Lock busy, attempt {attempt + 1}/3")
                time.sleep(10)
            
            return False
        except Exception as e:
            logger.error(f"Lock error: {e}")
            return False
    
    def release(self):
        """Release lock"""
        try:
            if self.sheet.acell(self.lock_cell).value == self.lock_id:
                self.sheet.update_acell(self.lock_cell, '')
                logger.info("✓ Lock released")
        except Exception as e:
            logger.error(f"Lock release error: {e}")

# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class LeadProcessor:
    
    def __init__(self):
        self.client = None
        self.leads_sheet = None
        self.results_sheet = None
        self.processed_phones = set()
        self.processed_names = set()
    
    def connect(self) -> bool:
        """Connect to Google Sheets"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                Config.CREDENTIALS_FILE, scope
            )
            self.client = gspread.authorize(creds)
            
            spreadsheet = self.client.open(Config.SPREADSHEET_NAME)
            self.leads_sheet = spreadsheet.worksheet(Config.LEADS_WORKSHEET)
            self.results_sheet = spreadsheet.worksheet(Config.RESULTS_WORKSHEET)
            
            logger.info("✓ Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            return False
    
    def load_processed_data(self):
        """Load existing processed data"""
        try:
            results = self.results_sheet.get_all_values()[1:]  # Skip header
            
            for row in results:
                if len(row) > 5 and row[5]:  # Phone Number column
                    normalized = IndianPhoneValidator.normalize(row[5])
                    if normalized:
                        self.processed_phones.add(normalized)
                
                if len(row) > 0 and row[0]:  # Restaurant Name column
                    self.processed_names.add(row[0].strip().lower())
            
            logger.info(f"Loaded: {len(self.processed_phones)} phones, {len(self.processed_names)} names")
        except Exception as e:
            logger.error(f"Error loading processed data: {e}")
    
    def should_process_lead(self, lead_row: list, row_num: int) -> Tuple[bool, str]:
        """Check if lead should be processed"""
        try:
            # Column indices (0-based)
            restaurant_name = lead_row[0].strip() if len(lead_row) > 0 else ''
            phone_raw = lead_row[3] if len(lead_row) > 3 else ''
            status = lead_row[5] if len(lead_row) > 5 else ''
            
            # Check 1: Already processed
            if status == "Complete":
                return False, "Status: Complete"
            
            # Check 2: Empty name
            if not restaurant_name:
                return False, "Empty name"
            
            # Check 3: Duplicate name
            if restaurant_name.lower() in self.processed_names:
                return False, "Duplicate name"
            
            # Check 4: Validate phone
            is_valid, normalized, error = IndianPhoneValidator.validate(phone_raw)
            if not is_valid:
                return False, f"Invalid phone: {error}"
            
            # Check 5: Duplicate phone
            if normalized in self.processed_phones:
                return False, "Duplicate phone"
            
            return True, "OK"
        except Exception as e:
            return False, f"Error: {e}"
    
    def process_lead(self, lead_row: list, row_num: int) -> bool:
        """Process single lead"""
        try:
            # Extract data (adjust indices to match your LEADS sheet)
            restaurant_name = lead_row[0].strip()
            rating = lead_row[1] if len(lead_row) > 1 else ''
            website_url = lead_row[2] if len(lead_row) > 2 else ''
            phone_raw = lead_row[3]
            
            # Validate and normalize phone
            _, normalized_phone, _ = IndianPhoneValidator.validate(phone_raw)
            
            logger.info(f"Processing: {restaurant_name} ({normalized_phone})")
            
            # Prepare row for RESULTS sheet
            # Match your exact column structure
            new_row = [
                restaurant_name,     # A: Restaurant Name
                "",                  # B: Flow Analysis (empty, filled by scraper)
                "",                  # C: Builder Prompt (empty, filled by scraper)
                "",                  # D: Outreach Status (empty initially)
                "",                  # E: Preview URL (empty, filled by n8n)
                normalized_phone,    # F: Phone Number (10 digits)
                "",                  # G: Message ID
                "",                  # H: Last_Message_Sent
                "",                  # I: Last_Message_Content
                "",                  # J: Last_Reply_Received
                "",                  # K: Last_Reply_Content
                "0",                 # L: Follow_Up_Count
                "FALSE",             # M: Auto_Acknowledge_Sent
                "FALSE",             # N: VAPI_Call_Scheduled
                ""                   # O: Call_Scheduled_At
            ]
            
            # Append to RESULTS
            self.results_sheet.append_row(new_row)
            logger.info(f"✓ Added to RESULTS")
            time.sleep(Config.API_RATE_LIMIT)
            
            # Update LEADS status
            self.leads_sheet.update_cell(row_num, 6, "Complete")  # Column F (Status)
            logger.info(f"✓ Marked Complete in LEADS")
            time.sleep(Config.API_RATE_LIMIT)
            
            # Update cache
            self.processed_phones.add(normalized_phone)
            self.processed_names.add(restaurant_name.lower())
            
            return True
        except Exception as e:
            logger.error(f"✗ Error: {e}")
            try:
                self.leads_sheet.update_cell(row_num, 6, f"Error: {str(e)[:50]}")
            except:
                pass
            return False
    
    def run(self) -> int:
        """Main execution"""
        if not self.connect():
            return 0
        
        lock = SheetLock(self.leads_sheet, Config.LOCK_CELL, Config.LOCK_TIMEOUT)
        if not lock.acquire():
            logger.error("Could not acquire lock")
            return 0
        
        processed_count = 0
        
        try:
            self.load_processed_data()
            
            all_leads = self.leads_sheet.get_all_values()[1:]  # Skip header
            logger.info(f"Total leads: {len(all_leads)}")
            
            for idx, lead in enumerate(all_leads, start=2):  # Row 2 = first data
                if processed_count >= Config.DAILY_LEAD_LIMIT:
                    logger.info(f"Daily limit reached ({Config.DAILY_LEAD_LIMIT})")
                    break
                
                should_process, reason = self.should_process_lead(lead, idx)
                
                if not should_process:
                    logger.debug(f"Row {idx}: SKIP - {reason}")
                    continue
                
                if self.process_lead(lead, idx):
                    processed_count += 1
                    logger.info(f"Progress: {processed_count}/{Config.DAILY_LEAD_LIMIT}")
            
            logger.info(f"=== Complete: {processed_count} leads processed ===")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            lock.release()
        
        return processed_count

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    logger.info("="*60)
    logger.info("Lead Processor Started")
    logger.info(f"Daily Limit: {Config.DAILY_LEAD_LIMIT}")
    logger.info("="*60)
    
    processor = LeadProcessor()
    processed = processor.run()
    
    logger.info("="*60)
    logger.info(f"Complete - Processed {processed} leads")
    logger.info("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
