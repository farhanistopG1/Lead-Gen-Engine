#!/usr/bin/env python3
"""
Lead Processing Engine - Restaurant Outreach Automation
Scrapes websites, analyzes with AI, and syncs to Google Sheets
"""

import subprocess
import sys

# Auto-install dependencies
REQUIRED_PACKAGES = [
    'gspread',
    'oauth2client',
    'requests',
    'beautifulsoup4',
    'google-generativeai',
    'python-dotenv'
]

def install_dependencies():
    """Auto-install required packages"""
    for package in REQUIRED_PACKAGES:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install', 
                '--break-system-packages', package
            ])

install_dependencies()

# Now import everything
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from bs4 import BeautifulSoup
import google.generativeai as genai
import time
import re
import os
from datetime import datetime, timedelta
from typing import Tuple, Optional, Set
import logging
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    # Google Sheets
    CREDENTIALS_FILE = 'credentials.json'
    SPREADSHEET_NAME = 'Lead Generation'
    LEADS_WORKSHEET = 'LEADS'
    RESULTS_WORKSHEET = 'RESULTS'
    
    # Processing
    DAILY_LEAD_LIMIT = 3
    API_RATE_LIMIT = 2
    
    # Lock mechanism
    LOCK_CELL = 'Z1'
    LOCK_TIMEOUT = 300
    
    # Gemini API
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBnGqSLcu6rKf_6-ZDYvl2eNRHHGnKa-_w')
    
    # Scraping
    SCRAPE_TIMEOUT = 30
    MAX_CONTENT_LENGTH = 50000

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
# INDIAN PHONE VALIDATION
# ============================================================================

class IndianPhoneValidator:
    """Validates and normalizes Indian phone numbers"""
    
    INVALID_STRINGS = {'not found', 'n/a', 'na', '', 'none', 'null'}
    MOBILE_PREFIXES = {'6', '7', '8', '9'}
    
    @staticmethod
    def normalize(phone: str) -> str:
        """Extract 10-digit mobile number"""
        if not phone:
            return ''
        
        digits = ''.join(filter(str.isdigit, str(phone)))
        
        # Remove country code
        if digits.startswith('91') and len(digits) == 12:
            digits = digits[2:]
        
        # Remove leading 0
        if digits.startswith('0'):
            digits = digits[1:]
        
        return digits[-10:] if len(digits) >= 10 else digits
    
    @staticmethod
    def validate(phone: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Validate and return (is_valid, normalized, error)"""
        if not phone:
            return False, None, "Empty"
        
        phone_lower = str(phone).strip().lower()
        
        if phone_lower in IndianPhoneValidator.INVALID_STRINGS:
            return False, None, f"Invalid literal: '{phone}'"
        
        normalized = IndianPhoneValidator.normalize(phone)
        
        if len(normalized) != 10:
            return False, None, f"Invalid length: {len(normalized)}"
        
        if normalized[0] not in IndianPhoneValidator.MOBILE_PREFIXES:
            return False, None, f"Landline/Invalid prefix: {normalized[0]}"
        
        return True, normalized, None

# ============================================================================
# LOCK MECHANISM
# ============================================================================

class SheetLock:
    def __init__(self, sheet, lock_cell: str, timeout: int):
        self.sheet = sheet
        self.lock_cell = lock_cell
        self.timeout = timeout
        self.lock_id = None
    
    def acquire(self) -> bool:
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
                        logger.info("Lock acquired")
                        return True
                
                time.sleep(10)
            
            return False
        except Exception as e:
            logger.error(f"Lock error: {e}")
            return False
    
    def release(self):
        try:
            if self.sheet.acell(self.lock_cell).value == self.lock_id:
                self.sheet.update_acell(self.lock_cell, '')
        except:
            pass

# ============================================================================
# WEB SCRAPER
# ============================================================================

class WebScraper:
    @staticmethod
    def scrape_website(url: str) -> Optional[str]:
        """Scrape website content"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=Config.SCRAPE_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove script and style tags
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()
            
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            
            if len(text) > Config.MAX_CONTENT_LENGTH:
                text = text[:Config.MAX_CONTENT_LENGTH]
            
            return text
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return None

# ============================================================================
# AI ANALYZER
# ============================================================================

class AIAnalyzer:
    def __init__(self):
        genai.configure(api_key=Config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def analyze_flaws(self, content: str, restaurant_name: str) -> Optional[str]:
        """Analyze website for strategic flaws"""
        try:
            prompt = f"""Analyze this restaurant website for {restaurant_name}.

Website Content:
{content[:10000]}

Identify the TOP 3 CRITICAL website flaws that hurt their business. For each flaw:
1. Describe the specific problem
2. Explain the strategic business impact

Format: Brief, business-focused analysis (300-500 words).
Focus on: UX issues, missing CTAs, poor mobile experience, slow loading, confusing navigation, missing contact info."""
            
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Flaw analysis error: {e}")
            return None
    
    def generate_builder_prompt(self, content: str, restaurant_name: str, flaw_analysis: str) -> Optional[str]:
        """Generate website builder prompt"""
        try:
            prompt = f"""Create an AI website builder prompt for {restaurant_name}.

Website Content:
{content[:10000]}

Identified Flaws:
{flaw_analysis}

Generate a detailed prompt that:
1. Describes the ideal modern website
2. Addresses each identified flaw
3. Specifies features, design, and user experience
4. Includes SEO and performance requirements

Format: Comprehensive technical prompt (400-600 words) for an AI to build a perfect restaurant website."""
            
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Builder prompt error: {e}")
            return None

# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class LeadProcessor:
    def __init__(self):
        self.client = None
        self.leads_sheet = None
        self.results_sheet = None
        self.processed_phones: Set[str] = set()
        self.processed_names: Set[str] = set()
        self.scraper = WebScraper()
        self.analyzer = AIAnalyzer()
    
    def connect(self) -> bool:
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
            
            logger.info("Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def load_processed_data(self):
        """Load existing processed data to prevent duplicates"""
        try:
            results = self.results_sheet.get_all_values()[1:]
            
            for row in results:
                if len(row) > 5 and row[5]:
                    normalized = IndianPhoneValidator.normalize(row[5])
                    if normalized:
                        self.processed_phones.add(normalized)
                
                if len(row) > 0 and row[0]:
                    self.processed_names.add(row[0].strip().lower())
            
            logger.info(f"Loaded: {len(self.processed_phones)} phones, {len(self.processed_names)} names")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
    
    def should_process_lead(self, lead_row: list) -> Tuple[bool, str]:
        """Check if lead should be processed"""
        try:
            restaurant_name = lead_row[0].strip() if len(lead_row) > 0 else ''
            website_url = lead_row[2] if len(lead_row) > 2 else ''
            phone_raw = lead_row[3] if len(lead_row) > 3 else ''
            status = lead_row[5] if len(lead_row) > 5 else ''
            
            if status == "Complete":
                return False, "Already processed"
            
            if not restaurant_name:
                return False, "Empty name"
            
            if restaurant_name.lower() in self.processed_names:
                return False, "Duplicate name"
            
            if not website_url or 'no website' in website_url.lower():
                return False, "No website URL"
            
            is_valid, normalized, error = IndianPhoneValidator.validate(phone_raw)
            if not is_valid:
                return False, f"Invalid phone: {error}"
            
            if normalized in self.processed_phones:
                return False, "Duplicate phone"
            
            return True, "OK"
        except Exception as e:
            return False, f"Error: {e}"
    
    def process_lead(self, lead_row: list, row_num: int) -> bool:
        """Process single lead with scraping and AI analysis"""
        try:
            restaurant_name = lead_row[0].strip()
            website_url = lead_row[2]
            phone_raw = lead_row[3]
            
            _, normalized_phone, _ = IndianPhoneValidator.validate(phone_raw)
            
            logger.info(f"\nProcessing: {restaurant_name}")
            logger.info(f"URL: {website_url}")
            
            # Scrape website
            print("Scraping website...")
            content = self.scraper.scrape_website(website_url)
            if not content:
                raise Exception("Failed to scrape website")
            
            print(f"Scraped {len(content)} characters")
            
            # AI Analysis
            print("Starting AI analysis...")
            flaw_analysis = self.analyzer.analyze_flaws(content, restaurant_name)
            if not flaw_analysis:
                raise Exception("Failed to analyze flaws")
            
            print("Completed flaw analysis")
            
            builder_prompt = self.analyzer.generate_builder_prompt(
                content, restaurant_name, flaw_analysis
            )
            if not builder_prompt:
                raise Exception("Failed to generate builder prompt")
            
            print("Generated builder prompt")
            
            # Prepare row for RESULTS
            new_row = [
                restaurant_name,
                flaw_analysis,
                builder_prompt,
                "",                    # Outreach Status
                "",                    # Preview URL
                normalized_phone,
                "",                    # Message ID
                "",                    # Last_Message_Sent
                "",                    # Last_Message_Content
                "",                    # Last_Reply_Received
                "",                    # Last_Reply_Content
                "0",                   # Follow_Up_Count
                "FALSE",               # Auto_Acknowledge_Sent
                "FALSE",               # VAPI_Call_Scheduled
                ""                     # Call_Scheduled_At
            ]
            
            print("Saving results...")
            self.results_sheet.append_row(new_row)
            time.sleep(Config.API_RATE_LIMIT)
            
            self.leads_sheet.update_cell(row_num, 6, "Complete")
            time.sleep(Config.API_RATE_LIMIT)
            
            self.processed_phones.add(normalized_phone)
            self.processed_names.add(restaurant_name.lower())
            
            print(f"Successfully processed: {restaurant_name}\n")
            return True
            
        except Exception as e:
            logger.error(f"Processing error: {e}")
            try:
                self.leads_sheet.update_cell(row_num, 6, f"Error: {str(e)[:50]}")
            except:
                pass
            return False
    
    def run(self):
        """Main execution loop"""
        if not self.connect():
            return
        
        lock = SheetLock(self.leads_sheet, Config.LOCK_CELL, Config.LOCK_TIMEOUT)
        if not lock.acquire():
            logger.error("Could not acquire lock")
            return
        
        try:
            self.load_processed_data()
            
            all_leads = self.leads_sheet.get_all_values()[1:]
            
            processed = 0
            remaining = Config.DAILY_LEAD_LIMIT
            
            for idx, lead in enumerate(all_leads, start=2):
                if processed >= Config.DAILY_LEAD_LIMIT:
                    break
                
                should_process, reason = self.should_process_lead(lead)
                if not should_process:
                    continue
                
                if self.process_lead(lead, idx):
                    processed += 1
                    remaining -= 1
                    print(f"Progress: {processed}/{Config.DAILY_LEAD_LIMIT}")
                    print(f"Remaining: {remaining}")
            
            if processed >= Config.DAILY_LEAD_LIMIT:
                print(f"Daily limit reached ({Config.DAILY_LEAD_LIMIT} leads)")
                
                tomorrow = datetime.now() + timedelta(days=1)
                next_run = tomorrow.replace(hour=0, minute=1, second=0)
                print(f"Sleeping until {next_run.strftime('%Y-%m-%d %H:%M:%S')}\n")
            
        except KeyboardInterrupt:
            print("\nStopped by user\n")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            lock.release()
            print("Processor stopped")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    processor = LeadProcessor()
    processor.run()
