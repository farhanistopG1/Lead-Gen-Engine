# --- Filename: master_control.py (Final, Robust Version) ---
import gspread
import subprocess
import sys
import os
import time

# --- CONFIGURATION ---
SPREADSHEET_NAME = "Lead Gen Engine"

def main():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        time.sleep(60) # Wait before retry on connection error
        return

    all_campaigns = campaigns_worksheet.get_all_records()
    active_campaign = None
    active_campaign_row = None
    
    # This loop now correctly finds the first valid, unprocessed campaign
    for i, campaign in enumerate(all_campaigns):
        status = campaign.get('Status', '').strip()
        area = campaign.get('Area', '').strip()
        
        # If area is empty OR status is NOT empty, skip to the next row
        if not area or status:
            continue
            
        # If we reach here, we've found a valid campaign to run
        active_campaign = campaign
        active_campaign_row = i + 2
        break # Exit the loop, we only want to run one campaign
    
    # This block only runs if the loop finished and NO campaigns were found
    if not active_campaign:
        print("🎉 All campaigns are complete! Waiting 60 seconds before checking again...")
        time.sleep(60)
        return
    
    # --- The rest of your script remains the same ---
    campaign_query = active_campaign['Area']
    print(f"🎯 Running Campaign: {campaign_query} (Row: {active_campaign_row})")
    
    python_path = sys.executable
    hunter_script = os.path.join(script_dir, "Apihuntermaps.py")
        
    try:
        print("\n🚀 Starting Hunter Script...")
        subprocess.run(
            [python_path, hunter_script, campaign_query],
            check=True, timeout=300
        )
        print(f"\n✅ Hunter script finished. Marking campaign as Complete...")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Complete")
        
    except Exception as e:
        print(f"❌ Error during hunter subprocess: {e}")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Error - Check Logs")

    print("Master Control cycle complete.")

if __name__ == "__main__":
    main()
