# --- Filename: master_control.py (Final Version) ---
import gspread
import subprocess
import sys
import os
import time # --- FIX: Import the time module ---

# --- CONFIGURATION ---
SPREADSHEET_NAME = "Lead Gen Engine"

def main():
    # Get credentials path and connect to sheets
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'gspread_credentials.json')
        gc = gspread.service_account(filename=creds_path)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        time.sleep(60) # Wait before exiting on error
        return

    all_campaigns = campaigns_worksheet.get_all_records()
    active_campaign = None
    active_campaign_row = None
    
    for i, campaign in enumerate(all_campaigns):
        if campaign.get('Status', '').strip() == '':
            active_campaign = campaign
            active_campaign_row = i + 2
            break
    
    # --- FIX: Add a pause if no campaigns are found ---
    if not active_campaign:
        print("🎉 All campaigns are complete! Waiting 60 seconds before checking again...")
        time.sleep(60) # Pauses for 60 seconds to prevent API rate limiting
        return
    # --- FIX END ---
    
    campaign_query = active_campaign.get('Area', '')
    if not campaign_query:
        print(f"Skipping row {active_campaign_row}, 'Area' is empty.")
        return

    print(f"🎯 Running Campaign: {campaign_query}")

    # Run the hunter script
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
        print(f"❌ Error running hunter script: {e}")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Error - Check Logs")

    print("Master Control cycle complete.")

if __name__ == "__main__":
    main()
