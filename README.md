
## Description - Test Dataset Generation
Each run creates a number of sign-in sessions equal to the number of users multiplied by (Poisson(5) + 1). In plain terms, each user averages about six interactive login sessions. Each of those sessions produces around four to five events (one interactive login plus several token-refresh calls, and occasionally an impossible-travel event). Taken together, that works out to roughly 6 × 4.5 ≈ 27 documents per user. So if you set num_users = 1000, you’ll end up with approximately 27 000 total documents in a single run.

#### Ingest into Elasticsearch
```powershell
curl.exe -k -X POST "https://<host>:9200/<what_you_want_to_call_index>/_bulk?pretty" `
     -H "Content-Type: application/x-ndjson" `
     -H "Authorization: ApiKey <api_key>" `
     --data-binary "@./bulk_synthetic_entra_signin.ndjson"
```

- You now have a richly varied, production-like dataset ready for:
  - Transform + scripted-metric → Haversine speed calculation
  - ML jobs → “rare location for user”, “high token refresh rate”, etc.
  - Dashboards → compare normal vs impossible travel sessions


## What it does when you press Run             |

| Phase                         | What happens                                                                                                        | Typical runtime on a laptop |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| 1. Build user roster          | Creates `num_users` emails and assigns 1–3 “home” cities each.                                                      | < 0.1 s                     |
| 2. Generate interactive logins| Each user draws `Poisson(5) + 1` to decide sessions—≈ 6 interactive logins per user, spread over the past year.    | 1–2 s                       |
| 3. Add token-refresh events   | For each session, insert 1–6 silent refreshes (≈ 3.5 on average), so each session yields ~4–5 total events.        | 2 s                         |
| 4. Inject “impossible travel” | 3 % of sessions get a second login ~2 minutes later from a different city (anonymousIP flag).                     | negligible                  |
| 5. Write NDJSON               | Write ~27 × `num_users` total documents (≈ 27 000 if `num_users=1000`) to `bulk_synthetic_entra_signin.ndjson`.     | 1–3 s                       |



## Packages

| Package    | One-line purpose                                            | What you get                                                                                                     | Typical use cases                                                                            |
| ---------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Faker**  | Generate fake—but realistic—data of almost any type.        | `fake.name()`, `fake.ipv4_public()`, `fake.company()`, 50+ locales, custom providers.                            | Test datasets, UI demos, populating dev databases, anonymising PII in examples.              |
| **pandas** | High-level tabular data analysis (“Excel + SQL in Python”). | `DataFrame` and `Series` objects, powerful joins, group-by, time-series resampling, I/O to CSV/JSON/Parquet/SQL. | ETL pipelines, exploratory data analysis, feature engineering for ML, business reporting.    |
| **NumPy**  | Fast n-dimensional arrays and vectorised math.              | `ndarray` structure, linear algebra, random number generation, broadcasting, universal functions (ufuncs).       | Numerical computing, statistics, image processing, building blocks for SciPy/PyTorch/pandas. |
| **pytz**   | Accurate cross-platform IANA time-zone handling.            | `pytz.timezone("Europe/London")`, conversion between local and UTC, daylight-saving rules.                       | Scheduling apps, log-file timestamp normalisation, any system juggling multiple regions.     |


## Elasticsearch Integration - Microsoft Entra ID

```json
{
    "@timestamp": "2019-10-18T09:45:48.072Z",
    "azure": {
        "correlation_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
        "resource": {
            "id": "/tenants/8a4de8b5-095c-47d0-a96f-a75130c61d53/providers/Microsoft.aadiam",
            "provider": "Microsoft.aadiam"
        },
        "signinlogs": {
            "category": "SignInLogs",
            "identity": "Test LTest",
            "operation_name": "Sign-in activity",
            "operation_version": "1.0",
            "properties": {
                "app_display_name": "Office 365",
                "app_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
                "client_app_used": "Browser",
                "conditional_access_status": "notApplied",
                "correlation_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
                "created_at": "2019-10-18T04:45:48.0729893-05:00",
                "device_detail": {
                    "browser": "Chrome 77.0.3865",
                    "device_id": "",
                    "operating_system": "MacOs"
                },
                "id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
                "is_interactive": false,
                "original_request_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
                "processing_time_ms": 239,
                "risk_detail": "none",
                "risk_level_aggregated": "none",
                "risk_level_during_signin": "none",
                "risk_state": "none",
                "service_principal_id": "",
                "status": {
                    "error_code": 50140
                },
                "token_issuer_name": "",
                "token_issuer_type": "AzureAD",
                "user_display_name": "Test LTest",
                "user_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
                "user_principal_name": "test@elastic.co"
            },
            "result_description": "This error occurred due to 'Keep me signed in' interrupt when the user was signing-in.",
            "result_signature": "None",
            "result_type": "50140"
        },
        "tenant_id": "8a4de8b5-095c-47d0-a96f-a75130c61d53"
    },
    "client": {
        "ip": "1.1.1.1"
    },
    "cloud": {
        "provider": "azure"
    },
    "ecs": {
        "version": "8.11.0"
    },
    "event": {
        "action": "Sign-in activity",
        "category": [
            "authentication"
        ],
        "duration": 0,
        "id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
        "ingested": "2021-09-14T17:20:47.736433526Z",
        "kind": "event",
        "original": "{\"Level\":\"4\",\"callerIpAddress\":\"1.1.1.1\",\"category\":\"SignInLogs\",\"correlationId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"durationMs\":0,\"identity\":\"Test LTest\",\"location\":\"FR\",\"operationName\":\"Sign-in activity\",\"operationVersion\":\"1.0\",\"properties\":{\"appDisplayName\":\"Office 365\",\"appId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"clientAppUsed\":\"Browser\",\"conditionalAccessStatus\":\"notApplied\",\"correlationId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"createdDateTime\":\"2019-10-18T04:45:48.0729893-05:00\",\"deviceDetail\":{\"browser\":\"Chrome 77.0.3865\",\"deviceId\":\"\",\"operatingSystem\":\"MacOs\"},\"id\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"ipAddress\":\"1.1.1.1\",\"isInteractive\":false,\"location\":{\"city\":\"Champs-Sur-Marne\",\"countryOrRegion\":\"FR\",\"geoCoordinates\":{\"latitude\":48.12341234,\"longitude\":2.12341234},\"state\":\"Seine-Et-Marne\"},\"originalRequestId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"processingTimeInMilliseconds\":239,\"riskDetail\":\"none\",\"riskLevelAggregated\":\"none\",\"riskLevelDuringSignIn\":\"none\",\"riskState\":\"none\",\"servicePrincipalId\":\"\",\"status\":{\"errorCode\":50140,\"failureReason\":\"This error occurred due to 'Keep me signed in' interrupt when the user was signing-in.\"},\"tokenIssuerName\":\"\",\"tokenIssuerType\":\"AzureAD\",\"userDisplayName\":\"Test LTest\",\"userId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"userPrincipalName\":\"test@elastic.co\"},\"resourceId\":\"/tenants/8a4de8b5-095c-47d0-a96f-a75130c61d53/providers/Microsoft.aadiam\",\"resultDescription\":\"This error occurred due to 'Keep me signed in' interrupt when the user was signing-in.\",\"resultSignature\":\"None\",\"resultType\":\"50140\",\"tenantId\":\"8a4de8b5-095c-47d0-a96f-a75130c61d53\",\"time\":\"2019-10-18T09:45:48.0729893Z\"}",
        "outcome": "failure",
        "type": [
            "info"
        ]
    },
    "geo": {
        "city_name": "Champs-Sur-Marne",
        "country_iso_code": "FR",
        "country_name": "Seine-Et-Marne",
        "location": {
            "lat": 48.12341234,
            "lon": 2.12341234
        }
    },
    "log": {
        "level": "4"
    },
    "message": "This error occurred due to 'Keep me signed in' interrupt when the user was signing-in.",
    "related": {
        "ip": [
            "1.1.1.1"
        ]
    },
    "source": {
        "address": "1.1.1.1",
        "as": {
            "number": 13335,
            "organization": {
                "name": "Cloudflare, Inc."
            }
        },
        "geo": {
            "continent_name": "Oceania",
            "country_iso_code": "AU",
            "country_name": "Australia",
            "location": {
                "lat": -33.494,
                "lon": 143.2104
            }
        },
        "ip": "1.1.1.1"
    },
    "tags": [
        "preserve_original_event"
    ],
    "user": {
        "domain": "elastic.co",
        "full_name": "Test LTest",
        "id": "8a4de8b5-095c-47d0-a96f-a75130c61d53",
        "name": "test"
    }
}
```

## Elasticsearch Transform

### Buckets & Metrics — Core Ideas in Elasticsearch Aggregations

| Concept                | One-sentence definition                                                                                 |
| ---------------------- | ------------------------------------------------------------------------------------------------------- |
| **Bucket aggregation** | Splits the current result set into **sub-sets of documents** ( *buckets* ) based on a rule.             |
| **Metric aggregation** | Calculates a **number** (avg, max, scripted value, etc.) **inside each bucket**.                        |
| **Criterion**          | The *rule* that decides *which* documents land in a bucket (e.g., “same user ID”, “same calendar day”). |


### 1 Bucket Aggregations

“A bucket aggregation returns a list of buckets.
Each bucket is a collection of documents that match a specific criterion.” — Elasticsearch Guide

| Common bucket agg | Criterion it uses        | Example outcome                               |
| ----------------- | ------------------------ | --------------------------------------------- |
| `terms`           | each unique keyword      | one bucket per `user.name`                    |
| `date_histogram`  | calendar/time interval   | one bucket per **day**                        |
| `range`           | numeric range boundaries | buckets “0–999 km/h”, “1000+ km/h”            |
| `filters`         | Boolean conditions       | bucket “city = London”, bucket “city = Tokyo” |


***Why they matter***
Bucket aggs let you “group-by” without writing SQL or re-indexing.
Every further calculation (metrics, sub-buckets) is scoped ***per bucket***.


### 2 Metric Aggregations

| Metric agg                 | What it returns                        | Typical use                       |
| -------------------------- | -------------------------------------- | --------------------------------- |
| `avg`, `min`, `max`, `sum` | single number                          | average response time, max price  |
| `top_hits` / `top_metrics` | selected doc fields                    | newest log entry in bucket        |
| `scripted_metric`          | *anything you script* (number, object) | custom KPIs like **travel speed** |
| `percentiles`              | distribution points                    | 95ᵗʰ-percentile latency           |


A metric agg ***sees only the docs in its bucket***.

Example:

```json
"aggs": {
  "per_user": {
    "terms": { "field": "user.name.keyword" },
    "aggs": {
      "last_login": {
        "top_metrics": {
          "metrics": [{ "field": "@timestamp" }],
          "sort": { "@timestamp": "desc" }
        }
      }
    }
  }
}
```

### 3 Buckets inside Transforms

A ***Transform pivot*** is built on the same engine:

```jsonc
"group_by": {
  "user_name": { "terms": { "field": "user.name.keyword" } },
  "login_day": { "date_histogram": { "field": "@timestamp", "calendar_interval": "1d" } }
},
"aggregations": {
  "speed_kmh": { "scripted_metric": { … } }
}
```

- ***group_by ⇒*** bucket aggs
one bucket per user per calendar day
- ***speed_kmh ⇒*** metric agg
computes the highest travel speed inside that bucket

➡️ The transform ***writes one document per bucket*** to the destination index.


### 4 Rules of Thumb

1. Start with bucket aggs to segment the data.

2. Add metric aggs to compute useful numbers inside each segment.

3. Use nested buckets when you need multi-level grouping (e.g., user → day → hour).

4. Remember that order inside a bucket isn’t guaranteed unless you sort or use a ```date_histogram```.

5. Combine with a retention_policy (in transforms) to keep only recent buckets and avoid unbounded growth.

### Quick cheat sheet

```text
bucket  = “folder” of docs chosen by a rule
metric  = number(s) computed inside that folder
criterion = the rule itself (“same keyword”, “same 1-day interval”, …)
```

Understanding this pairing—***bucket ⇒ metric***—is the key to mastering Elasticsearch analytics, from simple counts to advanced scripted KPIs like impossible-travel speed.

***Examples***,

```json
PUT _transform/entra_impossible_travel
{
  "source": {
    "index": "azure_entra_signin_logs",
    "query": {                           
      "range": {
        "@timestamp": { "gte": "now-30d" }
      }
    }
  },

  "dest":   { "index": "entra_impossible_travel" },

  "pivot": {
    "group_by": {
      "user_name": {                     
        "terms": { "field": "user.name.keyword" }
      }
    },

    "aggregations": {
      "speed_kmh": {                    
        "scripted_metric": {
          "init_script": """
            state.prevTs  = null;
            state.prevLat = null;
            state.prevLon = null;
            state.maxSpd  = 0.0;
          """,
          "map_script": """
            def ts  = doc['@timestamp'].value.toInstant().toEpochMilli();
            def lat = doc['geo.location.lat'].value;
            def lon = doc['geo.location.lon'].value;

            if (state.prevTs != null) {
              double R = 6371;
              double dLat = Math.toRadians(lat - state.prevLat);
              double dLon = Math.toRadians(lon - state.prevLon);
              double a = Math.sin(dLat/2d)*Math.sin(dLat/2d) +
                         Math.cos(Math.toRadians(state.prevLat)) *
                         Math.cos(Math.toRadians(lat)) *
                         Math.sin(dLon/2d)*Math.sin(dLon/2d);
              double c  = 2d * Math.atan2(Math.sqrt(a), Math.sqrt(1d-a));
              double km = R * c;

              double hrs = (ts - state.prevTs) / 3.6e6;      // ms → h
              if (hrs > 0d) {
                double spd = km / hrs;
                if (spd > state.maxSpd) state.maxSpd = spd;  // keep the max
              }
            }
            state.prevTs  = ts;
            state.prevLat = lat;
            state.prevLon = lon;
          """,
          "combine_script": "return state.maxSpd;",
          "reduce_script" : "double m = 0; for (s in states) if (s > m) m = s; return m;"
        }
      },

      "last_ts": { "max": { "field": "@timestamp" } }, 

      "last_ip": {
        "top_metrics": {
          "metrics": [{ "field": "client.ip.keyword" }],
          "sort":    { "@timestamp": "desc" }
        }
      },
      "last_lat": {
        "top_metrics": {
          "metrics": [{ "field": "geo.location.lat" }],
          "sort":    { "@timestamp": "desc" }
        }
      },
      "last_lon": {
        "top_metrics": {
          "metrics": [{ "field": "geo.location.lon" }],
          "sort":    { "@timestamp": "desc" }
        }
      }
    }
  },

  "sync": { "time": { "field": "@timestamp", "delay": "60s" } }
}
```

```text

                  ┌─── transform bucket hierarchy ───┐
                  │  (group_by = user_name only)      │
                  │                                   │
Root (all data) ──┼────────────────────────────────────┐
                  │                                    │
                  │  user_name = "user001"  ──► scripted_metric
                  │                                    │
                  │  user_name = "user002"  ──► scripted_metric
                  │                                    │
                  │  user_name = "alice"    ──► scripted_metric
                  │                                    │
                  │  user_name = "bob"      ──► scripted_metric
                  │                                    │
                  │  … one bucket per distinct         │
                  │    user.name.keyword value         │
                  └────────────────────────────────────┘
```



```json
PUT _transform/entra_impossible_travel_daily
{
  "source": {
    "index": "azure_entra_signin_logs",
    "query": {                      
      "range": { "@timestamp": { "gte": "now-30d" } }
    }
  },

  "dest": { "index": "entra_impossible_travel_daily" },

  "pivot": {
    "group_by": {
      "user_name": {
        "terms": { "field": "user.name.keyword" }
      },
      "login_day": {                 
        "date_histogram": {
          "field": "@timestamp",
          "calendar_interval": "1d",
          "time_zone": "UTC"        
        }
      }
    },

    "aggregations": {
      "speed_kmh": {                 
        "scripted_metric": {
          "init_script": """
            state.prevTs  = null;
            state.prevLat = null;
            state.prevLon = null;
            state.maxSpd  = 0.0;
          """,
          "map_script": """
            def ts  = doc['@timestamp'].value.toInstant().toEpochMilli();
            def lat = doc['geo.location.lat'].value;
            def lon = doc['geo.location.lon'].value;

            if (state.prevTs != null) {
              double R = 6371;
              double dLat = Math.toRadians(lat - state.prevLat);
              double dLon = Math.toRadians(lon - state.prevLon);
              double a = Math.sin(dLat/2d)*Math.sin(dLat/2d) +
                         Math.cos(Math.toRadians(state.prevLat)) *
                         Math.cos(Math.toRadians(lat)) *
                         Math.sin(dLon/2d)*Math.sin(dLon/2d);
              double c  = 2d * Math.atan2(Math.sqrt(a), Math.sqrt(1d-a));
              double km = R * c;

              double hrs = (ts - state.prevTs) / 3.6e6;
              if (hrs > 0d) {
                double spd = km / hrs;
                if (spd > state.maxSpd) state.maxSpd = spd;
              }
            }
            state.prevTs  = ts;
            state.prevLat = lat;
            state.prevLon = lon;
          """,
          "combine_script": "return state.maxSpd;",
          "reduce_script" : "double m=0; for (s in states) if (s>m) m=s; return m;"
        }
      },

      "last_ts": { "max": { "field": "@timestamp" } },

      "last_ip": {
        "top_metrics": {
          "metrics": [{ "field": "client.ip.keyword" }],
          "sort": { "@timestamp": "desc" }
        }
      },
      "last_lat": {
        "top_metrics": {
          "metrics": [{ "field": "geo.location.lat" }],
          "sort": { "@timestamp": "desc" }
        }
      },
      "last_lon": {
        "top_metrics": {
          "metrics": [{ "field": "geo.location.lon" }],
          "sort": { "@timestamp": "desc" }
        }
      }
    }
  },

  "sync": { "time": { "field": "@timestamp", "delay": "60s" } },

  "retention_policy": {             
    "time": { "field": "login_day", "max_age": "30d" }
  }
}
```

```text
┌─ Bucket tree ──────────────────────────────────────────────────────────────┐
│                                                                            │
│  user_name = "user101"                                                     │
│  ├─ login_day = 2025-05-31 00:00 UTC  ──► scripted_metric ⇒ speed_kmh=1 234 │
│  ├─ login_day = 2025-06-01 00:00 UTC  ──► speed_kmh=0                       │
│  └─ login_day = 2025-06-02 00:00 UTC  ──► speed_kmh=8 900                   │
│                                                                            │
│  user_name = "user102"                                                     │
│  ├─ login_day = 2025-05-31 00:00 UTC  ──► speed_kmh=0                       │
│  └─ login_day = 2025-06-01 00:00 UTC  ──► speed_kmh=11 300                  │
│                                                                            │
│  user_name = "user103"                                                     │
│  ├─ login_day = 2025-06-01 00:00 UTC  ──► speed_kmh=0                       │
│  └─ login_day = 2025-06-02 00:00 UTC  ──► speed_kmh=0                       │
│                                                                            │
│  … one branch like this for every user that has logins in the last 30 days │
└────────────────────────────────────────────────────────────────────────────┘
```