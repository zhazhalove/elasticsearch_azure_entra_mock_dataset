"""
realistic_entra_signin_commented.py
-----------------------------------
Generate a production-like Azure Entra ID sign-in dataset
(≈50–60 k events) for testing transforms, ML, and “impossible travel”
detection in Elasticsearch/Kibana, and output Bulk‐compatible NDJSON.
"""

# ── 1. Standard and third-party imports ───────────────────────────────
import random, uuid, json, ipaddress           # stdlib helpers
from datetime import datetime, timedelta       # date math
from faker import Faker                        # fake but realistic values
import pandas as pd, pytz, numpy as np         # data wrangling & math

# ── 2. Initialise randomness for reproducibility ──────────────────────
fake = Faker()          # new Faker generator (default locale = en_US)
Faker.seed(42)          # same fake data every run ⇒ easier debugging
np.random.seed(42)      # same NumPy random stream

MAX_NUM_DAYS = 364 # 1 year of events

# ── 3. City catalogue: where logins originate ─────────────────────────
city_data = [
    # Google Cloud (US-East) and AWS us-east-1
    {"city": "New York",  "cc": "US", "lat": 40.7128,  "lon": -74.0060,
     "tz": "America/New_York",
     "cidrs": ["34.74.0.0/16",    # Google
               "52.56.0.0/15"]},  # AWS

    # AWS us-west-1 and Oracle OCI
    {"city": "San Francisco", "cc": "US", "lat": 37.7749, "lon": -122.4194,
     "tz": "America/Los_Angeles",
     "cidrs": ["13.56.0.0/16",    # AWS
               "129.146.0.0/17"]},# Oracle

    # AWS eu-west-2 and DigitalOcean LON1
    {"city": "London",  "cc": "GB", "lat": 51.5074, "lon": -0.1278,
     "tz": "Europe/London",
     "cidrs": ["3.8.0.0/15",      # AWS
               "167.99.128.0/17"]},# DigitalOcean

    # AWS eu-central-1 and Hetzner
    {"city": "Berlin",  "cc": "DE", "lat": 52.5200, "lon": 13.4050,
     "tz": "Europe/Berlin",
     "cidrs": ["18.194.0.0/15",   # AWS
               "162.55.0.0/16"]}, # Hetzner

    # AWS ap-northeast-1 and Alibaba
    {"city": "Tokyo",   "cc": "JP", "lat": 35.6895, "lon": 139.6917,
     "tz": "Asia/Tokyo",
     "cidrs": ["13.112.0.0/13",   # AWS (big /13)
               "47.244.0.0/16"]}, # Alibaba

    # AWS ap-southeast-2 and Google Sydney
    {"city": "Sydney",  "cc": "AU", "lat": -33.8688,"lon": 151.2093,
     "tz": "Australia/Sydney",
     "cidrs": ["3.104.0.0/14",    # AWS
               "35.244.0.0/16"]}, # Google

    # AWS ca-central-1 and Azure Canada East
    {"city": "Toronto", "cc": "CA", "lat": 43.6511, "lon": -79.3470,
     "tz": "America/Toronto",
     "cidrs": ["15.222.0.0/15",   # AWS
               "52.228.0.0/15"]}, # Azure

    # AWS sa-east-1 and Google São Paulo
    {"city": "São Paulo","cc": "BR","lat": -23.5505,"lon": -46.6333,
     "tz": "America/Sao_Paulo",
     "cidrs": ["18.228.0.0/14",   # AWS
               "35.247.0.0/17"]}, # Google

    # AWS ap-southeast-1 and DigitalOcean SG1
    {"city": "Singapore","cc": "SG","lat": 1.3521, "lon": 103.8198,
     "tz": "Asia/Singapore",
     "cidrs": ["13.228.0.0/15",   # AWS
               "128.199.128.0/17"]}, # DigitalOcean

    # AWS af-south-1 and Google Johannesburg
    {"city": "Johannesburg","cc": "ZA","lat": -26.2041,"lon": 28.0473,
     "tz": "Africa/Johannesburg",
     "cidrs": ["13.244.0.0/15",   # AWS
               "196.54.0.0/16"]}, # Google
]

# Helper: pick a random IP from a /24 block so IP-to-Geo is consistent.
def random_ip(cidr: str) -> str:
    net  = ipaddress.ip_network(cidr)
    # skip .0 (network) & .255 (broadcast); choose a host in between
    host = net.network_address + random.randint(10, net.num_addresses - 10)
    return str(host)

# ── 4. Create synthetic user population (500 accounts) ────────────────
num_users = 1000
all_users = []
for i in range(num_users):
    email = f"user{i}@example.com"
    # Each user has 1-3 “home” cities where they usually log in
    preferred_cities = random.sample(city_data, k=random.choice([1, 2, 3]))
    all_users.append(
        {
            "email": email,
            "uid": str(uuid.uuid4()),                   # stable user id
            "behavior": [c["city"] for c in preferred_cities],
        }
    )

# ── 5. Time helper: pick a realistic local login datetime ─────────────
def base_login_time(city: dict) -> datetime:
    """Return a timestamp in that city's timezone within the last MAX_NUM_DAYS,
    biased toward working-hours."""
    tz = pytz.timezone(city["tz"])

    # Random day in last MAX_NUM_DAYS
    dt = datetime.now(tz) - timedelta(days=random.randint(0, MAX_NUM_DAYS))

    # Choose hour: weekdays 07-19, weekends 09-16 (lighter traffic Sat/Sun)
    if dt.weekday() < 5:         # 0–4 = Mon-Fri
        hour = np.random.choice(range(7, 20),
                                p=np.array([8]*13)/104)   # flat-ish probs
    else:                        # 5-6 = Sat/Sun
        hour = np.random.choice(range(9, 17),
                                p=np.array([5]*8)/40)

    minute  = random.randint(0, 59)
    second  = random.randint(0, 59)
    return dt.replace(hour=hour, minute=minute, second=second, microsecond=0)

# ── 6. Event builder: returns one sign-in / token refresh document ─────
def make_event(user: dict,
               city: dict,
               dt: datetime,
               ip: str,
               anon: bool = False,          # mark as anonymous/VPN risk
               session_id: str | None = None,
               is_refresh: bool = False):   # True = silent token refresh
    log_id         = str(uuid.uuid4())
    timestamp_utc  = dt.astimezone(pytz.utc).isoformat()
    mfa_required   = (random.random() < 0.3) and not is_refresh

    # Assemble large nested JSON object in Azure Sign-in schema
    return {
        "@timestamp": timestamp_utc,
        "azure": {
            "correlation_id": log_id,
            "resource": {
                "id": f"/tenants/{log_id}/providers/Microsoft.aadiam",
                "provider": "Microsoft.aadiam",
            },
            "signinlogs": {
                "category": "SignInLogs",
                "identity": user["email"].split("@")[0],
                "operation_name": "Sign-in activity",
                "operation_version": "1.0",
                "properties": {
                    "app_display_name": "Office 365",
                    "app_id": session_id or log_id,     # sessions reuse id
                    "client_app_used": random.choice(["Browser", "MobileApp", "Electron"]),
                    "conditional_access_status": "success" if mfa_required else "notApplied",
                    "correlation_id": log_id,
                    "created_at": timestamp_utc,
                    "device_detail": {
                        "browser": random.choice(["Chrome", "Edge", "Firefox", "Safari"]),
                        "device_id": str(uuid.uuid4()) if random.random() > 0.3 else "",
                        "operating_system": random.choice(["Windows 11", "macOS", "Android", "iOS"]),
                    },
                    # Basic metadata
                    "id": log_id,
                    "is_interactive": not is_refresh,
                    "original_request_id": log_id,
                    "processing_time_ms": random.randint(120, 500),
                    # Risk flags
                    "risk_detail": "anonymousIP" if anon else "none",
                    "risk_level_aggregated": "none",
                    "risk_level_during_signin": "none",
                    "risk_state": "none",
                    "status": {"error_code": 0},
                    # User identity info
                    "token_issuer_type": "AzureAD",
                    "user_display_name": user["email"].split("@")[0],
                    "user_id": user["uid"],
                    "user_principal_name": user["email"],
                    # Optional MFA detail
                    **({"mfaDetail": {"method": "PhoneAppNotification"}} if mfa_required else {}),
                },
                "result_description": "Login succeeded",
                "result_signature": "None",
                "result_type": "0",
            },
            "tenant_id": log_id,
        },
        # ECS-like top-level fields
        "client": {"ip": ip},
        "cloud":  {"provider": "azure"},
        "ecs":    {"version": "8.11.0"},
        "event": {
            "action": "tokenRefresh" if is_refresh else "Sign-in activity",
            "category": ["authentication"],
            "id": log_id,
            "kind": "event",
            "outcome": "success",
            "type": ["info"],
        },
        # Geo coords (used by Haversine distance later)
        "geo": {
            "city_name": city["city"],
            "country_iso_code": city["cc"],
            "location": {"lat": city["lat"], "lon": city["lon"]},
        },
        # Source.* mirrors geo for IP data
        "source": {
            "ip": ip,
            "geo": {
                "country_iso_code": city["cc"],
                "location": {"lat": city["lat"], "lon": city["lon"]},
            },
        },
        # Short user object for ECS dashboards
        "user": {
            "domain": "example.com",
            "full_name": user["email"].split("@")[0],
            "id": user["uid"],
            "name": user["email"].split("@")[0],
        },
    }

# ── 7. Build the full dataset (~50k events) ───────────────────────────
events = []
for user in all_users:
    # Each account does Poisson(5)+1 primary logins over two weeks (≈1-6)
    primary_count = np.random.poisson(5) + 1
    for _ in range(primary_count):
        # 90 % in preferred city list, 10 % random travel
        if random.random() < 0.9:
            city_name = random.choice(user["behavior"])
        else:
            city_name = random.choice([c["city"] for c in city_data])
        city = next(c for c in city_data if c["city"] == city_name)

        dt_local   = base_login_time(city)
        ip         = random_ip(random.choice(city["cidrs"]))
        session_id = str(uuid.uuid4())

        # 7.1 Interactive login (the one that shows MFA, etc.)
        events.append(make_event(user, city, dt_local, ip, session_id=session_id))

        # 7.2 1–6 silent token refreshes within same session
        for _ in range(random.randint(1, 6)):
            delta = timedelta(minutes=random.randint(5, 120))
            events.append(
                make_event(user, city, dt_local + delta, ip,
                           session_id=session_id, is_refresh=True)
            )

        # 7.3 Occasionally insert an “impossible travel” login
        #     3 % of sessions → new city, 2 min later, ‘anonymousIP’ risk
        if random.random() < 0.03:
            far_city = random.choice([c for c in city_data if c["city"] != city["city"]])
            events.append(
                make_event(
                    user,
                    far_city,
                    dt_local + timedelta(minutes=2),
                    random_ip(random.choice(far_city["cidrs"])),
                    anon=True,
                    session_id=str(uuid.uuid4()),
                )
            )

# ── 8. Inspect and save to Bulk‐compatible NDJSON ─────────────────────
df = pd.json_normalize(events)
print("Total events generated:", len(df))
print(df[["@timestamp", "user.name", "geo.city_name", "event.action"]].head(10))

# Persist to disk → now with an "index" action line before each document
with open("bulk_synthetic_entra_signin.ndjson", "w", encoding="utf-8") as f:
    for doc in events:
        # Write the action/metadata line. Since we're calling:
        #   POST /azure_entra_signin_logs/_bulk
        # ES will automatically index each following document into azure_entra_signin_logs.
        f.write(json.dumps({"index": {}}) + "\n")
        # Now write the actual document JSON:
        f.write(json.dumps(doc) + "\n")

print("File written: bulk_synthetic_entra_signin.ndjson")

