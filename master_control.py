# --- Filename: master_control.py (Production Ready - Final Version) ---
import gspread
import subprocess
import sys
import os
import time
import json
import logging
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURATION
# ============================================================================

SPREADSHEET_NAME = "Lead Gen Engine"
MAX_RETRIES = 3
RETRY_DELAY = 10

# Hunter settings
HUNTER_TIMEOUT = 1800  # 30 minutes

# Loop timing
LOOP_DELAY = 60  # Check every 60 seconds
CAMPAIGN_SUCCESS_DELAY = 1800  # 30 minutes between successful campaigns
CAMPAIGN_FAILURE_DELAY = 300  # 5 minutes retry on failure

# Daily limits
MAX_CAMPAIGNS_PER_DAY = 5  # Adjust as needed
CAMPAIGN_TRACKING_FILE = "daily_campaigns_log.json"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('master_control.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CAMPAIGN TRACKING FUNCTIONS
# ============================================================================

def load_campaign_log():
    """Load daily campaign counter from file"""
    if os.path.exists(CAMPAIGN_TRACKING_FILE):
        try:
            with open(CAMPAIGN_TRACKING_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"date": "", "processed_count": 0}
    return {"date": "", "processed_count": 0}

def reset_if_new_day(log_data):
    """Reset counter if it's a new day"""
    today = datetime.now().strftime("%Y-%m-%d")
    if log_data["date"] != today:
        logger.info(f"🌅 New day! Resetting campaign counter.")
        log_data["date"] = today
        log_data["processed_count"] = 0
    return log_data

def save_campaign_log(log_data):
    """Save campaign counter to file"""
    with open(CAMPAIGN_TRACKING_FILE, 'w') as f:
        json.dump(log_data, f)

def check_daily_limit():
    """Check if daily campaign limit reached"""
    campaign_log = load_campaign_log()
    campaign_log = reset_if_new_day(campaign_log)
    
    if campaign_log["processed_count"] >= MAX_CAMPAIGNS_PER_DAY:
        logger.info(f"🎯 Daily limit reached: {campaign_log['processed_count']}/{MAX_CAMPAIGNS_PER_DAY} campaigns")
        
        # Calculate sleep time until tomorrow
        now = datetime.now()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0)
        sleep_seconds = (tomorrow - now).total_seconds()
        
        logger.info(f"😴 Sleeping until {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(sleep_seconds)
        return False
    
    return True

def increment_campaign_count():
    """Increment today's campaign counter"""
    campaign_log = load_campaign_log()
    campaign_log = reset_if_new_day(campaign_log)
    campaign_log["processed_count"] += 1
    save_campaign_log(campaign_log)
    logger.info(f"📊 Daily progress: {campaign_log['processed_count']}/{MAX_CAMPAIGNS_PER_DAY} campaigns")

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def connect_to_sheets(retry_count=0):
    """Connect to Google Sheets with exponential backoff retry"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        
        if not os.path.exists(creds_path):
            logger.error(f"❌ Credentials file not found: {creds_path}")
            return None, None
        
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
        
        logger.info("✅ Connected to Google Sheets")
        return spreadsheet, campaigns_worksheet
        
    except gspread.exceptions.APIError as e:
        logger.error(f"⚠️ Google Sheets API Error: {e}")
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (2 ** retry_count)
            logger.info(f"🔄 Retrying in {wait_time}s... (Attempt {retry_count + 1}/{MAX_RETRIES})")
            time.sleep(wait_time)
            return connect_to_sheets(retry_count + 1)
        return None, None
        
    except Exception as e:
        logger.error(f"❌ Connection error: {e}")
        return None, None

# ============================================================================
# CAMPAIGN DISCOVERY
# ============================================================================

def find_active_campaign(campaigns_worksheet):
    """Find first unprocessed campaign with duplicate detection"""
    try:
        all_campaigns = campaigns_worksheet.get_all_records()
        
        if not all_campaigns:
            logger.info("📋 No campaigns found in sheet")
            return None, None
        
        # Track already processed areas
        processed_areas = set()
        
        # First pass: collect all completed areas
        for campaign in all_campaigns:
            status = str(campaign.get('Status', '')).strip()
            area = str(campaign.get('Area', '')).strip().lower()
            
            if status and area:
                processed_areas.add(area)
        
        # Second pass: find first unprocessed campaign
        for i, campaign in enumerate(all_campaigns):
            status = str(campaign.get('Status', '')).strip()
            area = str(campaign.get('Area', '')).strip()
            area_lower = area.lower()
            
            # Skip if no area OR already has status
            if not area or status:
                continue
            
            # Check for duplicate
            if area_lower in processed_areas:
                campaign_row = i + 2
                logger.warning(f"⚠️ Duplicate area detected: {area} (Row {campaign_row})")
                campaigns_worksheet.update_cell(campaign_row, 2, "Duplicate - Skipped")
                continue
            
            # Found valid campaign
            campaign_row = i + 2
            logger.info(f"🎯 Active campaign found: {area} (Row {campaign_row})")
            return campaign, campaign_row
        
        logger.info("✨ All campaigns complete!")
        return None, None
        
    except Exception as e:
        logger.error(f"❌ Error finding campaign: {e}")
        return None, None

# ============================================================================
# HUNTER SCRIPT EXECUTION
# ============================================================================

def run_hunter_script(campaign_query, script_dir):
    """Execute Apihuntermaps.py with comprehensive error handling"""
    python_path = sys.executable
    hunter_script = os.path.join(script_dir, "Apihuntermaps.py")
    
    if not os.path.exists(hunter_script):
        logger.error(f"❌ Hunter script not found: {hunter_script}")
        return False, "Script not found"
    
    logger.info(f"🚀 Starting Hunter Script for: {campaign_query}")
    
    try:
        process = subprocess.Popen(
            [python_path, hunter_script, campaign_query],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate(timeout=HUNTER_TIMEOUT)
        
        if process.returncode == 0:
            logger.info(f"✅ Hunter script completed successfully")
            
            # Log last few lines of output
            if stdout:
                lines = stdout.strip().split('\n')
                for line in lines[-10:]:
                    logger.debug(f"   {line}")
            
            return True, "Success"
        else:
            logger.error(f"❌ Hunter script failed with return code: {process.returncode}")
            if stderr:
                logger.error(f"Error output: {stderr[:300]}")
            return False, f"Exit code {process.returncode}"
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏱️ Hunter script timeout after {HUNTER_TIMEOUT}s")
        process.kill()
        return False, "Timeout"
        
    except FileNotFoundError:
        logger.error(f"❌ Python interpreter or script not found")
        return False, "File not found"
        
    except Exception as e:
        logger.error(f"❌ Unexpected error running hunter: {e}")
        return False, str(e)

# ============================================================================
# SHEET UPDATES
# ============================================================================

def update_campaign_status(campaigns_worksheet, row, status):
    """Update campaign status with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            campaigns_worksheet.update_cell(row, 2, status)
            logger.info(f"📝 Campaign status updated: {status}")
            return True
        except gspread.exceptions.APIError as e:
            if '429' in str(e):
                wait_time = (2 ** attempt) * 5
                logger.warning(f"⚠️ Rate limit hit, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Error updating status: {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return False
    
    logger.error(f"❌ Failed to update status after {MAX_RETRIES} attempts")
    return False

def count_new_leads(spreadsheet):
    """Count leads with 'Pending' status in LEADS sheet"""
    try:
        leads_worksheet = spreadsheet.worksheet("LEADS")
        all_values = leads_worksheet.get_all_values()
        
        # Count "Pending" in Status column (Column F = index 5)
        pending_count = 0
        for row in all_values[1:]:  # Skip header row
            if len(row) > 5 and row[5].strip().lower() == 'pending':
                pending_count += 1
        
        return pending_count
    except Exception as e:
        logger.warning(f"Could not count leads: {e}")
        return 0

# ============================================================================
# MAIN CAMPAIGN PROCESSOR
# ============================================================================

def process_campaign():
    """Single campaign processing cycle"""
    logger.info("=" * 70)
    logger.info(f"🎬 Cycle started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # Check daily limit
    if not check_daily_limit():
        return False
    
    # Connect to Google Sheets
    spreadsheet, campaigns_worksheet = connect_to_sheets()
    if not campaigns_worksheet:
        logger.error("❌ Failed to connect to Google Sheets. Waiting 60s...")
        return False
    
    # Find active campaign
    active_campaign, campaign_row = find_active_campaign(campaigns_worksheet)
    if not active_campaign:
        logger.info("💤 No campaigns to process. Waiting 60s...")
        return False
    
    # Count leads before processing
    leads_before = count_new_leads(spreadsheet)
    
    # Run Hunter script
    campaign_query = active_campaign['Area']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    success, message = run_hunter_script(campaign_query, script_dir)
    
    # Count leads after processing
    time.sleep(3)  # Wait for sheet to update
    leads_after = count_new_leads(spreadsheet)
    new_leads = leads_after - leads_before
    
    # Update campaign status
    if success:
        status = f"Complete - {new_leads} leads" if new_leads > 0 else "Complete - 0 leads"
        logger.info(f"🎉 Campaign '{campaign_query}' completed! Added {new_leads} leads")
        
        update_campaign_status(campaigns_worksheet, campaign_row, status)
        increment_campaign_count()
        
        logger.info(f"😴 Waiting {CAMPAIGN_SUCCESS_DELAY // 60} minutes before next campaign...")
        time.sleep(CAMPAIGN_SUCCESS_DELAY)
    else:
        status = f"Error - {message}"
        logger.error(f"💥 Campaign '{campaign_query}' failed: {message}")
        
        update_campaign_status(campaigns_worksheet, campaign_row, status)
        
        logger.info(f"⏳ Retrying in {CAMPAIGN_FAILURE_DELAY // 60} minutes...")
        time.sleep(CAMPAIGN_FAILURE_DELAY)
    
    logger.info("=" * 70)
    logger.info("✅ Cycle complete")
    logger.info("=" * 70)
    
    return True

# ============================================================================
# 24/7 MAIN LOOP
# ============================================================================

def main_loop():
    """Infinite loop for 24/7 operation"""
    logger.info("🚀 Master Control starting in 24/7 mode")
    logger.info(f"📂 Working directory: {os.getcwd()}")
    logger.info(f"🐍 Python interpreter: {sys.executable}")
    logger.info(f"⚙️  Daily campaign limit: {MAX_CAMPAIGNS_PER_DAY}")
    logger.info(f"⏱️  Delay between campaigns: {CAMPAIGN_SUCCESS_DELAY // 60} minutes")
    
    while True:
        try:
            process_campaign()
            time.sleep(LOOP_DELAY)
            
        except KeyboardInterrupt:
            logger.info("\n⛔ Interrupted by user")
            break
            
        except Exception as e:
            logger.critical(f"💀 Critical error in main loop: {e}", exc_info=True)
            logger.info(f"🔄 Restarting in {LOOP_DELAY}s...")
            time.sleep(LOOP_DELAY)

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("\n👋 Master Control stopped by user")
        sys.exit(0)
