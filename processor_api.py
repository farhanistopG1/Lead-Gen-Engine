from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"

# --- Initialize Google Sheets ---
script_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(script_dir, 'gspread_credentials.json')
gc = gspread.service_account(filename=creds_path)
spreadsheet = gc.open(SPREADSHEET_NAME)
leads_ws = spreadsheet.worksheet("LEADS")
results_ws = spreadsheet.worksheet("RESULTS")

# --- Configure Gemini ---
genai.configure(api_key=GEMINI_API_KEY)
MODEL_NAME = "gemini-2.5-flash"

# --- Fetch first pending lead ---
all_leads = leads_ws.get_all_records()
lead_to_process = None
for lead in all_leads:
    if str(lead.get("Status")).strip().lower() == "pending":
        lead_to_process = lead
        break

if not lead_to_process:
    print("No pending leads found.")
    exit(0)

restaurant_name = lead_to_process["Restaurant Name"]
target_url = lead_to_process["Website URL"]

# --- Mark as Processing immediately ---
try:
    cell = leads_ws.find(restaurant_name)
    target_row = cell.row
    leads_ws.update_cell(target_row, 6, "Processing")  # Column 6 = Status
except Exception as e:
    print(f"Error finding/updating lead: {e}")
    exit(1)

print(f"Processing lead: {restaurant_name}")

# --- Scrape Website ---
body_html = ""
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    try:
        page.goto(target_url, timeout=60000)
        body_html = page.locator("body").inner_html()
    except Exception as e:
        print(f"Error scraping {target_url}: {e}")
        leads_ws.update_cell(target_row, 6, "Scraping Failed")
        exit(1)
    finally:
        browser.close()

# --- AI Analysis ---
try:
    prompt = f"""
    Analyze the HTML of {target_url}. Extract contact info (phone, email, socials)
    and provide 3 strategic website improvements.
    HTML: {body_html}
    """
    response = genai.generate_content(model=MODEL_NAME, prompt=prompt)
    analysis_text = response.text

    # --- Append results to RESULTS sheet ---
    results_ws.append_row([restaurant_name, analysis_text, "", "", ""])

    # --- Mark lead as Complete ---
    leads_ws.update_cell(target_row, 6, "Complete")
    print(f"✅ Successfully processed {restaurant_name}")

except Exception as e:
    print(f"AI processing failed for {restaurant_name}: {e}")
    leads_ws.update_cell(target_row, 6, "AI Failed")
    exit(1)
