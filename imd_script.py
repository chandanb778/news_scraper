import json
import re
from datetime import timedelta
from email.utils import parsedate_to_datetime
import requests
import os
try:
    import certifi
    CERTifi_PATH = certifi.where()
except Exception:
    CERTifi_PATH = None
import xml.etree.ElementTree as ET

# Optional: For Firebase integration
import firebase_admin
from firebase_admin import credentials, firestore
# Firestore client placeholder; remains None unless initialized below
db = None

# --- Configuration ---
FEED_URL = "https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml"

# =========================
# Firebase Initialization (GitHub Actions)
# =========================
db = None
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

if service_account_json:
    try:
        cred_dict = json.loads(service_account_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firestore initialized via GitHub Actions secret")
    except Exception as e:
        print("❌ Firestore init failed:", e)
else:
    print("❌ SERVICE_ACCOUNT_JSON not found. Firestore disabled.")



# Clear existing data first
# if db:
#     col_ref = db.collection(COLLECTION_NAME)
#     docs = col_ref.stream()
#     for doc in docs:
#         doc.reference.delete()
#     print("Old disaster_alerts collection cleared")



# Fetch the RSS feed (with ETag caching if desired)
try:
    headers = {"User-Agent": USER_AGENT}
    if CERTifi_PATH:
        resp = requests.get(FEED_URL, headers=headers, timeout=20, verify=CERTifi_PATH)
    else:
        resp = requests.get(FEED_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    xml_data = resp.text
except requests.exceptions.SSLError as e:
    print("SSL error fetching feed. Install certifi (`pip install certifi`) or set proper system certificates.", e)
    raise
except Exception as e:
    print("Error fetching feed:", e)
    raise
# Note: To support ETag caching, you would store request.getheader('ETag') 
# and supply it in the next request via the If-None-Match header.

# Parse the XML
root = ET.fromstring(xml_data)  # parse XML from string:contentReference[oaicite:4]{index=4}
items = root.findall('.//item')

alerts = []
seen = set()
for item in items:
    title = (item.find('title').text or "").strip()
    pub_date_str = (item.find('pubDate').text or "").strip()
    if not title or not pub_date_str:
        continue
    
    # Parse start datetime (RSS pubDate is in GMT)
    try:
        start_dt = parsedate_to_datetime(pub_date_str)  # returns timezone-aware UTC datetime
    except Exception:
        continue
    
    # Determine duration from title (default to hours)
    hours = None
    title_lower = title.lower()
    # English: "X hours" or "X days"
    m = re.search(r'(\d+)\s*(hour|hours|day|days)', title_lower)
    if m:
        num, unit = int(m.group(1)), m.group(2)
        hours = 24*num if 'day' in unit else num
    else:
        # Hindi: look for घंटा/घंटे/दिनो
        m = re.search(r'(\d+)\s*(घंटे|घंटों|घंटा|दिनों|दिन)', title)
        if m:
            num, unit = int(m.group(1)), m.group(2)
            hours = 24*num if 'दिन' in unit else num
    
    if hours is None:
        # If no duration found, skip this alert
        continue
    end_dt = start_dt + timedelta(hours=hours)
    
    # Extract location(s)
    locations = []
    if re.search(r'[\u0900-\u097F]', title):
        # Hindi title handling
        if 'आपके जनपद' in title:
            continue  # Skip generic "in your district" alerts
        if 'घंटों में ' in title:
            idx = title.find('घंटों में ')
            start = idx + len('घंटों में ')
            end = title.find(' में', start)
            loc_text = title[start:(end if end != -1 else None)]
            loc_text = loc_text.replace(' और ', ', ')
            parts = [p.strip(' ।,') for p in loc_text.split(',') if p.strip()]
            for part in parts:
                # Remove words like 'जिला' or 'जनपद'
                for word in [' ज़िला',' जिला',' जनपद',' जिलें',' ज़िला',' जिल्हा']:
                    if word in part:
                        part = part.split(word)[0].strip()
                if part:
                    locations.append(part)
    else:
        # English title handling
        idx = title_lower.find(' over ')
        if idx == -1:
            idx = title_lower.find(' places over ')
        if idx != -1:
            idx_start = title_lower.find('over ', idx) + len('over ')
            idx_end = len(title)
            # Try to cut off before phrases like "in next", "during", etc.
            for token in [' in next', ' during', ' for the next', ' for next', ' in past', ' up to']:
                tok = title_lower.find(token, idx_start)
                if tok != -1:
                    idx_end = min(idx_end, tok)
            loc_text = title[idx_start:idx_end]
            # Remove filler words
            loc_text = re.sub(r'\b(plain areas of|few places|places over|plain area of)\b', '', loc_text, flags=re.IGNORECASE)
            # Split by commas and "and"
            loc_text = loc_text.replace(' and ', ', ')
            parts = [p.strip(' .') for p in loc_text.split(',') if p.strip()]
            for part in parts:
                # Remove trailing state/district words
                part = re.sub(r'Districts of.*$', '', part, flags=re.IGNORECASE)
                part = re.sub(r'\bdistricts\b', '', part, flags=re.IGNORECASE)
                part = part.strip()
                if part:
                    locations.append(part)
    
    # Extract disaster type (one word)
    type_phrase = title
    # Remove any leading timestamp bracket info
    if type_phrase.startswith('('):
        type_phrase = type_phrase.split(')', 1)[-1].strip()
    if type_phrase.startswith('['):
        type_phrase = type_phrase.split(']', 1)[-1].strip()
    if re.search(r'[\u0900-\u097F]', type_phrase):
        # Hindi type ("कोहरा" etc.)
        if 'कोहरा' in type_phrase:
            type_word = 'कोहरा'
        else:
            # If no clear keyword, take last noun (fallback)
            words = type_phrase.strip().split()
            type_word = words[-1].strip(' ।,')
    else:
        # English type
        lower = type_phrase.lower()
        if ' with ' in lower:
            type_phrase = type_phrase.split(' with ')[0]
        if 'likely' in lower:
            # remove "is very likely" or "is likely"
            type_phrase = re.split(r'is\s*(?:very\s*)?likely', type_phrase, flags=re.IGNORECASE)[0]
        type_word = type_phrase.strip().split()[-1].strip(' .')
    
    # Add entries (one per location) if not duplicate
    for loc in locations:
        entry = (loc, type_word, start_dt.isoformat(), end_dt.isoformat())
        if entry not in seen:
            seen.add(entry)
            alerts.append({
                'location': loc,
                'type': type_word,
                'start': start_dt.isoformat(),
                'end': end_dt.isoformat()
            })

# Print results and write to JSON
print(json.dumps(alerts, ensure_ascii=False, indent=2))

with open('alerts_output.json', 'w', encoding='utf-8') as f:
    json.dump(alerts, f, ensure_ascii=False, indent=2)





# (Optional) Push to Firestore if initialized
if db:
    for alert in alerts:
        try:
            db.collection(COLLECTION_NAME).add(alert)
        except Exception as e:
            print("Error pushing alert to Firestore:", e)
else:
    print("Firestore client not initialized; skipping push. To enable, set SERVICE_ACCOUNT_FILE and initialize firebase_admin.")


