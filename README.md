# Impossible‑Travel Daily Transform — **Tutorial README**
*Demystifying Elasticsearch Transforms + Scripted‑Metric Aggregations (8.x)*  

---

## 0 Table of Contents
1. [Why do we need this?](#1)  
2. [What *is* a transform?](#2)  
3. [Full transform listing (annotated)](#3)  
4. [Scripted‑Metric — fundamental concepts](#4)  
5. [Document life‑cycle — step‑by‑step walkthrough](#5)  
6. [Index‑sorting vs query‑sorting](#6)  
7. [How to query the roll‑up index](#7)  
8. [Performance hints & common pitfalls](#8)  
9. [Mini FAQ](#9)  

---

<a name="1"></a>
## 1 Why do we need this?
*“Impossible travel”* is an identity‑threat technique where an attacker reuses a
token from one geo‑location while the legitimate user is somewhere else.
Brute‑forcing these patterns out of raw logs is expensive:

* Millions of sign‑in documents per month  
* Geo distance math for every possible pair  
* Need to persist historical fast‑hops for alerting / dashboards

**Solution:** a *transform* that roll‑ups data *once* and stores **one document
per user‑day** with the *fastest* speed detected.

---

<a name="2"></a>
## 2 What *is* a transform in Elasticsearch?
> *A persistent task that reads documents, groups them, runs aggregations, and
> writes the result to a new index — continuously or batch.*

### 2.1 Key concepts
| Concept | In our example |
|---------|----------------|
| **Source** | `azure_entra_signin_logs` (raw Entra events) |
| **Dest**   | `entra_impossible_travel_daily` (roll‑up) |
| **Pivot**  | “group_by + aggregations” |
| **Sync**   | Incremental mode — keep reading new docs |
| **Retention** | Drop roll‑ups older than 30 days |

Transforms run **server‑side** — no Logstash or external ETL required.

---

<a name="3"></a>
## 3 Full transform listing (heavily commented)

```jsonc
PUT _transform/entra_impossible_travel_daily      // ➊ task id
{
  /* ---------- 3.1 SOURCE  ---------- */
  "source": {
    "index": "azure_entra_signin_logs",            // ➋ where to read
    "query": { "range": { "@timestamp": { "gte": "now-30d" } } }
    /* If shards are NOT index‑sorted, uncomment ▼
       "sort": [{ "@timestamp": "asc" }]           // guarantees chronologic feed
    */
  },

  /* ---------- 3.2 DESTINATION  ---------- */
  "dest": { "index": "entra_impossible_travel_daily" },

  /* ---------- 3.3 PIVOT  ---------- */
  "pivot": {

    /* 3.3.1 GROUP‑BY: one bucket = one user × one UTC day */
    "group_by": {
      "user_name": {
        "terms": { "field": "user.name.keyword" }  // exact term
      },
      "login_day": {
        "date_histogram": {
          "field": "@timestamp",
          "calendar_interval": "1d",
          "time_zone": "UTC"
        }
      }
    },

    /* 3.3.2 AGGREGATIONS */
    "aggregations": {

      /* (A) CORE METRIC: fastest hop */
      "speed_kmh": { "scripted_metric": { /* see § 4 */ } },

      /* (B) Convenience metadata */
      "last_ts" : { "max" : { "field": "@timestamp" } },
      "last_ip" : { "top_metrics": {
          "metrics": [{ "field": "client.ip.keyword" }],
          "sort": { "@timestamp": "desc" } } },
      "last_lat": { "top_metrics": {
          "metrics": [{ "field": "geo.location.lat" }],
          "sort": { "@timestamp": "desc" } } },
      "last_lon": { "top_metrics": {
          "metrics": [{ "field": "geo.location.lon" }],
          "sort": { "@timestamp": "desc" } } }
    }
  },

  /* ---------- 3.4 CONTINUOUS SYNC  ---------- */
  "sync": { "time": { "field": "@timestamp", "delay": "60s" } },

  /* ---------- 3.5 RETENTION  ---------- */
  "retention_policy": {
    "time": { "field": "login_day", "max_age": "30d" }
  }
}
```
Legend  
➊ Name of the transform task  
➋ You may point to an alias to swap indices without touching the task

---

<a name="4"></a>
## 4 Scripted‑Metric — fundamental concepts

| Stage          | Runs **where**   | Input                         | You write… | Purpose |
|----------------|------------------|-------------------------------|------------|---------|
| `init_script`  | Each **shard × bucket** | _nothing_ (blank state) | Field in `state` map | Initialise scratch pad |
| `map_script`   | Once **per doc** | Current doc, `state` map      | Speed math | Update shard‑local metrics |
| `combine_script` | End of shard    | Final `state` map            | `return` a tiny Object | Shrink data crossing network |
| `reduce_script` | Transform node   | `states[]` = every shard’s return | Global merge logic | Produce **one** number per bucket |

**Golden rule:** `state` never leaves the shard; only the object you
`return` in `combine_script` becomes an element of `states[]`.

---

<a name="5"></a>
## 5 Document life‑cycle — step‑by‑step

### 5.1 Shard layout (example)
| Node | Primary shards of `azure_entra_signin_logs` |
|------|--------------------------------------------|
| **Node A** | `shard‑0`, `shard‑2` |
| **Node B** | `shard‑1` |

### 5.2 Sample bucket
```
user_name = "alice"
login_day = 2025‑06‑12
```
Docs:  
* shard‑0 → 08:00 NYC, 12:00 Chicago  
* shard‑1 → 16:00 Paris  

#### Stage‑by‑stage animation  

```
init_script      (shard‑0)      state = {prev: null, max:0}
map_script #1    (08:00)  →     state.prev=null → just seed
map_script #2    (12:00)  →     distance=1145 km, Δt=4 h, 286 km/h  ⇒ state.max=286

combine_script   (shard‑0)      returns {max:286, last:{12:00,Chi}}

init_script      (shard‑1)      state = {prev:null, max:0}
map_script #1    (16:00)  →     only doc, state.max stays 0
combine_script   (shard‑1)      returns {max:0,   last:{16:00,Par}}

reduce_script    (task node)    iterate states[] in order:
                                • keep max(286,0)=286
                                • compute hop Chicago→Paris ≈665 km/h > 286 → keep 665
                                returns 665
```

Result document (rolled‑up):
```json
{
  "user_name" : "alice",
  "login_day" : "2025‑06‑12T00:00:00Z",
  "speed_kmh" : 665.1,
  "last_ts"   : "2025‑06‑12T16:00:00Z",
  "last_ip"   : "203.0.113.7",
  …
}
```

---

<a name="6"></a>
## 6 Index‑sorting vs Query‑sorting

| Approach | Pros | Cons | Syntax |
|----------|------|------|--------|
| **Index‑sort** (`index.sort.field`) | • Zero runtime cost<br>• Map‑script sees perfect order | Requires recreate / re‑index | Template in § 3.1 |
| **Query‑sort** (`"sort":[{"@timestamp":"asc"}]`) | No re‑index | • CPU/heap each run<br>• Large scroll pages slower | Inside `"source"` |

If neither is used, Lucene gives docs in *segment order* (basically random).

---

<a name="7"></a>
## 7 How to interrogate the roll‑up index

```http
# Who broke the sound barrier yesterday?
GET entra_impossible_travel_daily/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "login_day": "2025‑06‑13" } },
        { "range": { "speed_kmh": { "gt": 1000 } } }
      ]
    }
  },
  "_source": ["user_name","speed_kmh","last_ip"],
  "sort": [{ "speed_kmh": "desc" }]
}
```

---

<a name="8"></a>
## 8 Performance hints & pitfalls

* **Shard size** — smaller shards (5–10 GB) finish map‑phase faster.
* **`page_size`** transform setting — raise if you have thousands of docs per user‑day.
* **Geo lookup** — do in ingest pipeline, *not* in map‑script (which has no IP).
* **Script Sandboxing** — Painless runs on data nodes; infinite loops = crashed task.

---

<a name="9"></a>
## 9 Mini FAQ

<details>
<summary>Q: Can I emit *all* hop speeds instead of just max?</summary>

No — scripted‑metric must return a single value per bucket.  
Store all hops in a nested field during ingest if you need full path history.
</details>

<details>
<summary>Q: What about users with no geo?</summary>

Add an `exists` filter for `geo.location.lat` in `source.query` **or** guard
against `doc['geo.location.lat'].empty` inside `map_script`.
</details>

<details>
<summary>Q: Can I run this in batch once a night?</summary>

Yes.  Omit `"sync"` and use `POST _transform/<id>/_start?timeout=...` from a cron job.
</details>
