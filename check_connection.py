# test_connection.py
import requests
import sseclient
import json

URL = "https://wikimedia.org"
HEADERS = {"User-Agent": "DiagnosticClient/1.0 (student@domain.com)"}

print("🧪 STEP 1: Attempting direct HTTP handshake connection...")
try:
    response = requests.get(URL, stream=True, headers=HEADERS, timeout=10)
    print(f"📡 HTTP Response Status Code: {response.status_code}")
    
    if response.status_code != 200:
        print("❌ Handshake rejected by server.")
        exit()
        
    print("\n🧪 STEP 2: Binding SSE client parser...")
    client = sseclient.SSEClient(response)
    
    print("🚀 STEP 3: Reading first 3 live items from Wikipedia stream...")
    count = 0
    for event in client.events():
        if event.event == 'message' and event.data:
            change = json.loads(event.data)
            if change.get('server_name') == 'en.wikipedia.org':
                print(f"   📥 Live Event Catch [{count+1}]: '{change.get('title')}' by {change.get('user')}")
                count += 1
                if count >= 3:
                    break
                    
    print("\n✅ DIAGNOSTIC PASSED: Network and stream libraries are working perfectly!")

except Exception as e:
    print(f"\n❌ DIAGNOSTIC CRASHED: Found the hidden bug: {e}")
