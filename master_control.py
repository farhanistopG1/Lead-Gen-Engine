# --- Filename: master_control_simple.py ---
"""
Simple Master Control - Runs each campaign once
No lead counting needed since each specific query gets good results
"""
import gspread
import subprocess
import sys
import os

# --- CONFIGURATION ---
SPREADSHEET_NAME = "Lead Gen Engine"

def main():
    # Get credentials path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    
    # Connect to sheets
    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
    
    # Get all campaigns
    all_campaigns = campaigns_worksheet.get_all_records()
    
    # Find first campaign with empty Status
    active_campaign = None
    active_campaign_row = None
    
    for i, campaign in enumerate(all_campaigns):
        # Check if Status is empty
        status = campaign.get('Status', '').strip()
        area = campaign.get('Area', '').strip()
        
        # Skip if Area is empty or Status is not empty
        if not area:
            continue
        if status:  # Already has a status (Complete, etc)
            continue
        
        # Found an active campaign!
        active_campaign = campaign
        active_campaign_row = i + 2  # +2 for header row and 0-indexing
        break
    
    # Check if we found a campaign
    if not active_campaign:
        print("=" * 60)
        print("🎉 All campaigns are complete!")
        print("=" * 60)
        return
    
    # Get campaign details
    campaign_query = active_campaign['Area']
    target = active_campaign.get('Target', 20)
    
    print("=" * 60)
    print(f"🎯 Running Campaign:")
    print(f"   Query: {campaign_query}")
    print(f"   Target: {target}")
    print(f"   Row: {active_campaign_row}")
    print("=" * 60)
    
    # Run the hunter script
    python_path = sys.executable
    hunter_script = os.path.join(script_dir, "Apihuntermaps.py")
    
    try:
        print("\n🚀 Starting Hunter Script...")
        result = subprocess.run(
            [python_path, hunter_script, campaign_query],
            check=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        print("\n--- Hunter Output ---")
        print(result.stdout)
        print("-------------------\n")
        
        # Mark campaign as complete
        print(f"✅ Marking campaign as Complete...")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Complete")
        print(f"✅ Campaign marked Complete in row {active_campaign_row}")
        
    except subprocess.TimeoutExpired:
        print("⚠️  Hunter script timed out (>5 minutes)")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Timeout - Retry")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running hunter script:")
        print(f"   Exit code: {e.returncode}")
        if e.stdout:
            print(f"   Output: {e.stdout}")
        if e.stderr:
            print(f"   Error: {e.stderr}")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Error - Check Logs")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Error")
    
    print("\n" + "=" * 60)
    print("Master Control Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
