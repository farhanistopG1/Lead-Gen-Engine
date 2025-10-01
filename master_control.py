# --- Filename: master_control.py (Final Version) ---
import gspread
import subprocess
import sys
import os # --- FIX: Import the os module ---

# --- CONFIGURATION ---
SPREADSHEET_NAME = "Lead Gen Engine"
DEFAULT_LEAD_TARGET = 20  # Used if 'Target' column is empty

def main():
    # --- FIX START: Use an absolute path for the credentials file ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    gc = gspread.service_account(filename=creds_path)
    # --- FIX END ---
    
    spreadsheet = gc.open(SPREADSHEET_NAME)
    campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
    leads_worksheet = spreadsheet.worksheet("LEADS")

    all_campaigns = campaigns_worksheet.get_all_records()
    active_campaign_area = None
    active_campaign_row = None
    active_campaign_target = DEFAULT_LEAD_TARGET

    # Find first campaign with empty Status
    for i, campaign in enumerate(all_campaigns):
        if campaign.get('Status', '').strip() == '':
            active_campaign_area = campaign.get('Area', '').strip()
            
            # Skip if Area is empty
            if not active_campaign_area:
                continue
            
            active_campaign_row = i + 2  # +2 for header row and 0-indexing
            
            # Get target from sheet, use default if not specified
            target = campaign.get('Target', '')
            if target and str(target).strip():
                try:
                    active_campaign_target = int(target)
                except ValueError:
                    active_campaign_target = DEFAULT_LEAD_TARGET
            else:
                active_campaign_target = DEFAULT_LEAD_TARGET
            
            break

    if not active_campaign_area:
        print("All campaigns are complete!")
        return

    print(f"Active campaign: {active_campaign_area}")
    print(f"Lead target: {active_campaign_target}")

    # Count leads for this specific area
    if " in " in active_campaign_area:
        area_to_match = active_campaign_area.split(" in ")[1].split(" with")[0].strip()
    else:
        area_to_match = active_campaign_area

    all_leads_in_area_col = leads_worksheet.col_values(7)  # Column G (Area)
    area_lead_count = all_leads_in_area_col.count(area_to_match)
    
    print(f"Current leads for this area: {area_lead_count}/{active_campaign_target}")

    if area_lead_count < active_campaign_target:
        print("Lead target not met. Running the Hunter script...")
        
        python_path = sys.executable  # Uses the correct venv python
        hunter_script = os.path.join(script_dir, "Apihuntermaps.py") # Use full path to hunter
        
        try:
            result = subprocess.run(
                [python_path, hunter_script, active_campaign_area],
                check=True,
                capture_output=True,
                text=True
            )
            print("--- Hunter Script Output ---")
            print(result.stdout)
            print("--------------------------")
        except subprocess.CalledProcessError as e:
            print(f"Error running hunter script: {e}")
            print(f"Output: {e.stdout}")
            print(f"Error Output: {e.stderr}")
    else:
        print("Lead target met. Marking campaign as complete.")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Complete")

if __name__ == "__main__":
    main()
