#!/usr/bin/env python3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from bs4 import BeautifulSoup
import google.generativeai as genai
import time
import re
from datetime import datetime, timedelta

# Configuration
CREDENTIALS_FILE = 'credentials.json'
SPREADSHEET_NAME = 'Lead Generation'
LEADS_SHEET = 'LEADS'
RESULTS_SHEET = 'RESULTS'
DAILY_LIMIT = 3
GEMINI_API_KEY = 'AIzaSyBnGqSLcu6rKf_6-ZDYvl2eNRHHGnKa-_w'

# Initialize Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

def normalize_phone(phone):
    """Extract 10-digit mobile number"""
    if not phone:
        return ''
    digits = ''.join(filter(str.isdigit, str(phone)))
    if digits.startswith('91') and len(digits) == 12:
        digits = digits[2:]
    if digits.startswith('0'):
        digits = digits[1:]
    return digits[-10:] if len(digits) >= 10 else digits

def is_valid_mobile(phone):
    """Check if valid Indian mobile number"""
    if not phone or str(phone).lower().strip() in ['not found', 'n/a', 'na', '', 'none']:
        return False
    
    normalized = normalize_phone(phone)
    
    # Must be 10 digits and start with 6/7/8/9 (mobile prefixes)
    if len(normalized) != 10:
        return False
    
    if normalized[0] not in ['6', '7', '8', '9']:
        return False
    
    return True

def connect_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SPREADSHEET_NAME)
    return spreadsheet.worksheet(LEADS_SHEET), spreadsheet.worksheet(RESULTS_SHEET)

def get_processed_data(results_sheet):
    """Get already processed phones and names"""
    results = results_sheet.get_all_values()[1:]  # Skip header
    
    processed_phones = set()
    processed_names = set()
    
    for row in results:
        if len(row) > 5 and row[5]:  # Phone column
            normalized = normalize_phone(row[5])
            if normalized:
                processed_phones.add(normalized)
        
        if len(row) > 0 and row[0]:  # Name column
            processed_names.add(row[0].strip().lower())
    
    return processed_phones, processed_names

def scrape_website(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        return text[:50000] if len(text) > 50000 else text
    except Exception as e:
        print(f"Scraping error: {e}")
        return None

def analyze_flaws(content, restaurant_name):
    try:
        prompt = f"""Analyze this restaurant website for {restaurant_name}.

Website Content:
{content[:10000]}

Identify the TOP 3 CRITICAL website flaws that hurt their business. For each flaw:
1. Describe the specific problem
2. Explain the strategic business impact

Format: Brief, business-focused analysis (300-500 words).
Focus on: UX issues, missing CTAs, poor mobile experience, slow loading, confusing navigation, missing contact info."""
        
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Analysis error: {e}")
        return None

def generate_builder_prompt(content, restaurant_name, flaw_analysis):
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
        
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Builder prompt error: {e}")
        return None

def process_leads():
    leads_sheet, results_sheet = connect_sheets()
    
    # Load already processed data
    processed_phones, processed_names = get_processed_data(results_sheet)
    print(f"Already processed: {len(processed_phones)} phones, {len(processed_names)} names")
    
    all_leads = leads_sheet.get_all_values()[1:]  # Skip header
    processed_count = 0
    
    for idx, lead in enumerate(all_leads, start=2):  # Row 2 = first data row
        if processed_count >= DAILY_LIMIT:
            break
        
        # Extract data
        restaurant_name = lead[0].strip() if len(lead) > 0 else ''
        website_url = lead[2] if len(lead) > 2 else ''
        phone_raw = lead[3] if len(lead) > 3 else ''
        status = lead[5] if len(lead) > 5 else ''
        
        # Skip if already processed
        if status == "Pending":
            # Check duplicates
            if restaurant_name.lower() in processed_names:
                print(f"Row {idx}: SKIP - Duplicate name: {restaurant_name}")
                continue
            
            # Validate phone
            if not is_valid_mobile(phone_raw):
                print(f"Row {idx}: SKIP - Invalid phone: {phone_raw}")
                leads_sheet.update_cell(idx, 6, f"Invalid Phone: {phone_raw}")
                time.sleep(2)
                continue
            
            normalized_phone = normalize_phone(phone_raw)
            
            if normalized_phone in processed_phones:
                print(f"Row {idx}: SKIP - Duplicate phone: {normalized_phone}")
                continue
            
            # Check website
            if not website_url or 'no website' in website_url.lower():
                print(f"Row {idx}: SKIP - No website")
                continue
            
            # Process lead
            print(f"\nProcessing: {restaurant_name}")
            print(f"URL: {website_url}")
            
            try:
                # Scrape
                print("Scraping website...")
                content = scrape_website(website_url)
                if not content:
                    raise Exception("Scraping failed")
                print(f"Scraped {len(content)} characters")
                
                # Analyze
                print("Starting AI analysis...")
                flaw_analysis = analyze_flaws(content, restaurant_name)
                if not flaw_analysis:
                    raise Exception("Analysis failed")
                print("Completed flaw analysis")
                
                builder_prompt = generate_builder_prompt(content, restaurant_name, flaw_analysis)
                if not builder_prompt:
                    raise Exception("Builder prompt failed")
                print("Generated builder prompt")
                
                # Save to RESULTS
                print("Saving results...")
                new_row = [
                    restaurant_name,
                    flaw_analysis,
                    builder_prompt,
                    "",           # Outreach Status
                    "",           # Preview URL
                    normalized_phone,
                    "",           # Message ID
                    "",           # Last_Message_Sent
                    "",           # Last_Message_Content
                    "",           # Last_Reply_Received
                    "",           # Last_Reply_Content
                    "0",          # Follow_Up_Count
                    "FALSE",      # Auto_Acknowledge_Sent
                    "FALSE",      # VAPI_Call_Scheduled
                    ""            # Call_Scheduled_At
                ]
                
                results_sheet.append_row(new_row)
                time.sleep(2)
                
                # Update LEADS status
                leads_sheet.update_cell(idx, 6, "Complete")
                time.sleep(2)
                
                # Update cache
                processed_phones.add(normalized_phone)
                processed_names.add(restaurant_name.lower())
                
                processed_count += 1
                print(f"Successfully processed: {restaurant_name}\n")
                print(f"Progress: {processed_count}/{DAILY_LIMIT}")
                print(f"Remaining: {DAILY_LIMIT - processed_count}")
                
            except Exception as e:
                print(f"Error processing {restaurant_name}: {e}")
                leads_sheet.update_cell(idx, 6, f"Error: {str(e)[:50]}")
                time.sleep(2)
    
    if processed_count >= DAILY_LIMIT:
        print(f"Daily limit reached ({DAILY_LIMIT} leads)")
        tomorrow = datetime.now() + timedelta(days=1)
        next_run = tomorrow.replace(hour=0, minute=1, second=0)
        print(f"Sleeping until {next_run.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print("Processor stopped")

if __name__ == "__main__":
    try:
        process_leads()
    except KeyboardInterrupt:
        print("\nStopped by user\n")
