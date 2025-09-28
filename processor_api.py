from playwright.sync_api import sync_playwright
import google.generativeai as genai
import gspread
import os
import time

# ---------------- CONFIG ----------------
GEMINI_API_KEY = "AIzaSyBzXE-mJpydq9jAsMiyspeTl_wKjwILs3I"
SPREADSHEET_NAME = "Lead Gen Engine"
MAX_LEADS_PER_RUN = 3
SHEET_UPDATE_DELAY = 1  # seconds between sheet updates to avoid rate limits

# ---------------- CONNECT TO SHEETS ----------------
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'gspread_credentials.json')
    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    leads_worksheet = spreadsheet.worksheet("LEADS")
    results_worksheet = spreadsheet.worksheet("RESULTS")
except Exception as e:
    print(f"Error connecting to Google Sheets: {e}")
    exit(1)

# ---------------- CONFIGURE GEMINI ----------------
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")
except Exception as e:
    print(f"Error configuring Gemini client: {e}")
    exit(1)

# ---------------- GET PENDING LEADS ----------------
all_leads = leads_worksheet.get_all_records()
leads_to_process = [lead for lead in all_leads if str(lead.get("Status")).strip() == "Pending"]

if not leads_to_process:
    print("No pending leads found.")
    exit(0)

processed_count = 0

# ---------------- PROCESS LEADS ----------------
for idx, lead in enumerate(leads_to_process, start=2):  # start=2 to match sheet row numbers
    if processed_count >= MAX_LEADS_PER_RUN:
        break

    restaurant_name = lead["Restaurant Name"]
    target_url = lead.get("Website URL", "").strip()

    if not target_url or target_url.lower() in ["no website found", ""]:
        leads_worksheet.update_cell(idx, 6, "Processing Error - Scraping Failed")
        continue

    print(f"--- Processing Lead: {restaurant_name} ---")

    # ---- SCRAPE WEBSITE ----
    body_html = ""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(target_url, timeout=60000)
                body_html = page.locator("body").inner_html()
            except Exception as e:
                print(f"Error scraping {target_url}: {e}")
                leads_worksheet.update_cell(idx, 6, "Processing Error - Scraping Failed")
                continue
            finally:
                browser.close()
    except Exception as e:
        print(f"Playwright error for {target_url}: {e}")
        leads_worksheet.update_cell(idx, 6, "Processing Error - Scraping Failed")
        continue

    # ---- AI ANALYSIS ----
    try:
        prompt1 = f"""
        Analyze the raw HTML of {target_url}. Perform two tasks:
        TASK 1: Extract Data (About Us, Phone, Email, Social Media).
        TASK 2: Provide a Strategic Analysis of 3 critical website flaws.
        HTML: {body_html}
        """
        response1 = model.generate_content(prompt1)
        flaw_analysis = response1.text

        prompt2 = f"""Based on the following website analysis, generate a detailed prompt for an AI website builder.
ANALYSIS: {flaw_analysis}"""
        response2 = model.generate_content(prompt2)
        builder_prompt = response2.text

        # ---- LOG TO SHEETS ----
        results_worksheet.append_row([restaurant_name, flaw_analysis, builder_prompt, "", ""])
        leads_worksheet.update_cell(idx, 6, "Analysis Complete")
        processed_count += 1
        print(f"✅ Successfully processed {restaurant_name}")
        time.sleep(SHEET_UPDATE_DELAY)
    except Exception as e:
        print(f"Error during AI chain or sheet update for {restaurant_name}: {e}")
        leads_worksheet.update_cell(idx, 6, "Processing Error - AI Failed")
        continue

print(f"Processor run complete! Processed {processed_count} leads.")
