# --- Filename: master_control.py ---
import gspread
import subprocess

# --- CONFIGURATION ---
SPREADSHEET_NAME = "Lead Gen Engine"
LEAD_TARGET = 30 # How many leads to find before an area is "complete"

def main():
    gc = gspread.service_account(filename="gspread_credentials.json")
    spreadsheet = gc.open(SPREADSHEET_NAME)
    campaigns_worksheet = spreadsheet.worksheet("CAMPAIGNS")
    leads_worksheet = spreadsheet.worksheet("LEADS")

    all_campaigns = campaigns_worksheet.get_all_records()
    active_campaign_area = None
    active_campaign_row = None

    for i, campaign in enumerate(all_campaigns):
        if campaign['Status'] == '':
            active_campaign_area = campaign['Area']
            active_campaign_row = i + 2
            break
    
    if not active_campaign_area:
        print("All campaigns are complete!")
        return

    print(f"Active campaign: {active_campaign_area}")

    all_leads = leads_worksheet.col_values(7)
    area_lead_count = all_leads.count(active_campaign_area.split(" in ")[1].split(" with")[0])

    print(f"Current leads for this area: {area_lead_count}/{LEAD_TARGET}")

    if area_lead_count < LEAD_TARGET:
        print("Lead target not met. Running the Hunter script...")
        # Use subprocess to call the hunter script with the area as an argument
        subprocess.run(["/opt/myapp/venv/bin/python", "/opt/myapp/Apihuntermaps.py", active_campaign_area])
    else:
        print("Lead target met. Marking campaign as complete.")
        campaigns_worksheet.update_cell(active_campaign_row, 2, "Complete")

if __name__ == "__main__":
    main()
