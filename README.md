# Impossible‑Travel Daily Transform — **Tutorial README**
*Demystifying Elasticsearch Transforms + Scripted‑Metric Aggregations (8.x)*  

---

## 0 Table of Contents
1. [Why do we need this?](#1)  
2. [What *is* a transform?](#2)
3. [line-by-line walkthrough of the ``map_script``](#3)
4. [``reduce_script`` — line-by-line walk-through](#4)
5. [Full transform listing (annotated)](#5)  
6. [Scripted‑Metric — fundamental concepts](#6)  
7. [Document life‑cycle — step‑by‑step walkthrough](#7)  
8. [Index‑sorting vs query‑sorting](#8)  
9. [How to query the roll‑up index](#9)  
11. [Performance hints & common pitfalls](#11)  
12. [Mini FAQ](#12)
13. [Resources](#13)

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
## 3  Full transform listing (heavily commented)

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
## 4  line-by-line walkthrough of the ```map_script```
```
1  def ts  = doc['@timestamp'].value.toInstant().toEpochMilli();
2  def lat = doc['geo.location.lat'].value;
3  def lon = doc['geo.location.lon'].value;

4  if (state.prev != null) {                 // skip the very first doc
5      double R    = 6371;                   // Earth radius (km)
6      double dLat = Math.toRadians(lat - state.prev.lat);
7      double dLon = Math.toRadians(lon - state.prev.lon);
8      double a = Math.sin(dLat/2d) * Math.sin(dLat/2d) +
9                 Math.cos(Math.toRadians(state.prev.lat)) *
10                Math.cos(Math.toRadians(lat)) *
11                Math.sin(dLon/2d) * Math.sin(dLon/2d);
12     double c  = 2d * Math.atan2(Math.sqrt(a), Math.sqrt(1d - a));
13     double km = R * c;

14     double hrs = (ts - state.prev.ts) / 3.6e6;   // ms → h
15     if (hrs > 0d) {
16         double spd = km / hrs;                    // km/h
17         if (spd > state.max) state.max = spd;     // keep record
18     }
19 }
20 state.prev = ['ts': ts, 'lat': lat, 'lon': lon]; // slide window
```


| #         | What it does                                                                                                                                                                                 | Key API & docs                                      |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| **1**     | Reads the `@timestamp` doc-value, converts to Java `Instant`, then to epoch **ms** for easy arithmetic.                                                                                      | *Accessing doc values* ([elastic.co][1])            |
| **2 & 3** | Pull latitude & longitude (doubles) from `geo.location.*` doc-values.                                                                                                                        | Same `doc['field'].value` access ([elastic.co][1])  |
| **4**     | If this isn’t the first point in the shard-bucket, compute a hop.                                                                                                                            | `state` Map initialized in `init_script`.           |
| **5**     | Constant Earth radius in kilometres.                                                                                                                                                         |                                                     |
| **6–13**  | **Haversine distance** calculation:<br>• `Math.toRadians` converts Δlat/Δlon to radians.<br>• `Math.sin`, `Math.cos`, `Math.atan2`, `Math.sqrt` are allowed Java-`Math` methods in Painless. | *Painless API – `java.lang.Math`* ([elastic.co][2]) |
| **14**    | Convert time delta from **ms** to **hours** (`/ 3.6 × 10⁶`).                                                                                                                                 |                                                     |
| **15**    | Guard against pathological negative or zero Δt (can only happen if events were mis-ordered).                                                                                                 |                                                     |
| **16**    | Speed = km / h.                                                                                                                                                                              |                                                     |
| **17**    | Keep the shard-local record in `state.max`.                                                                                                                                                  |                                                     |
| **18**    | End of hop logic.                                                                                                                                                                            |                                                     |
| **19**    | Close the `if` block.                                                                                                                                                                        |                                                     |
| **20**    | Store the *current* point so the next doc can compare against it (sliding-window).                                                                                                           |                                                     |

---

<a name="5"></a>
## 5 ``reduce_script`` — line-by-line walk-through
(context = scripted_metric aggregation inside a transform)


```
1  double globalMax = 0;
2  def    lastPoint = null;

3  for (seg in states) {                         // states[] = per-shard summary
4      if (seg.max > globalMax) globalMax = seg.max;

5      if (lastPoint != null && seg.last != null) {
6          /* -------- haversine distance -------- */
7          double R    = 6371;                   // km
8          double dLat = Math.toRadians(seg.last.lat - lastPoint.lat);
9          double dLon = Math.toRadians(seg.last.lon - lastPoint.lon);
10         double a = Math.sin(dLat/2d) * Math.sin(dLat/2d) +
11                    Math.cos(Math.toRadians(lastPoint.lat)) *
12                    Math.cos(Math.toRadians(seg.last.lat)) *
13                    Math.sin(dLon/2d) * Math.sin(dLon/2d);
14         double c  = 2d * Math.atan2(Math.sqrt(a), Math.sqrt(1d - a));
15         double km = R * c;

16         /* -------- convert to km/h ----------- */
17         double hrs = (seg.last.ts - lastPoint.ts) / 3.6e6;
18         if (hrs > 0d) {
19             double spd = km / hrs;
20             if (spd > globalMax) globalMax = spd;
21         }
22     }

23     if (seg.last != null) lastPoint = seg.last;   // carry forward
24 }
25 return globalMax;
```


|      Line | Code excerpt                        | What happens                                                                                                   | API / doc references                                      |
| --------: | ----------------------------------- | -------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
|     **1** | `double globalMax = 0;`             | Initialise result accumulator.                                                                                 | Primitive types in Painless — “Language Basics” → *Types* |
|     **2** | `def lastPoint = null;`             | Placeholder for the *tail* geo-point from the previous shard.                                                  | Dynamic typing & `def` — “Dynamic Types”                  |
|     **3** | `for (seg in states)`               | Iterate the array that Painless injects into **reduce context** (`states[]`).                                  | Scripted-metric reduce docs                               |
|     **4** | `if (seg.max > globalMax)`          | Keep the highest **intra-shard** speed.                                                                        | —                                                         |
|     **5** | `if (lastPoint …)`                  | Only compute cross-shard hop from second shard onward.                                                         | —                                                         |
|     **7** | `double R = 6371;`                  | Earth radius constant.                                                                                         | —                                                         |
|  **8–15** | `dLat … km`                         | **Haversine** distance; uses whitelisted `java.lang.Math` methods: `toRadians`, `sin`, `cos`, `atan2`, `sqrt`. | Painless API — `java.lang.Math` whitelist                 |
|    **17** | `double hrs = … / 3.6e6`            | Convert epoch-ms delta to hours (3 600 000 ms = 1 h).                                                          | —                                                         |
| **18–20** | Guard + speed calc + record update. | Protects against negative / zero Δt; updates `globalMax` if cross-shard hop is faster.                         | —                                                         |
|    **23** | `lastPoint = seg.last`              | Carry this shard’s trailing point so the next shard can stitch its hop.                                        | —                                                         |
|    **25** | `return globalMax;`                 | Single value emitted for the bucket; becomes `speed_kmh` field in the dest index.                              | Scripted-metric `reduce_script` return                    |
                                                               |

**Key take-aways**
1. states array = per-shard summaries produced by combine_script.
You choose the structure; the runtime just hands it back to you. 
elastic.co

2. Cross-shard hops are your responsibility.
Elasticsearch never re-orders or merges shard results — the reduce script must do that.

3. All math relies on java.lang.Math, which is part of the shared whitelist available in any Painless context. 
elastic.co

With this loop the transform captures both:

 - the fastest hop inside any single shard, and

 - the fastest hop that crosses shard boundaries,

yielding an accurate speed_kmh for every user-day bucket.

---

<a name="6"></a>
## 6 Scripted‑Metric — fundamental concepts

| Stage          | Runs **where**   | Input                         | You write… | Purpose |
|----------------|------------------|-------------------------------|------------|---------|
| `init_script`  | Each **shard × bucket** | _nothing_ (blank state) | Field in `state` map | Initialise scratch pad |
| `map_script`   | Once **per doc** | Current doc, `state` map      | Speed math | Update shard‑local metrics |
| `combine_script` | End of shard    | Final `state` map            | `return` a tiny Object | Shrink data crossing network |
| `reduce_script` | Transform node   | `states[]` = every shard’s return | Global merge logic | Produce **one** number per bucket |

**Golden rule:** `state` never leaves the shard; only the object you
`return` in `combine_script` becomes an element of `states[]`.

---

<a name="7"></a>
## 7 Document life‑cycle — step‑by‑step

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

<a name="8"></a>
## 8 Index‑sorting vs Query‑sorting

| Approach | Pros | Cons | Syntax |
|----------|------|------|--------|
| **Index‑sort** (`index.sort.field`) | • Zero runtime cost<br>• Map‑script sees perfect order | Requires recreate / re‑index | Template in § 3.1 |
| **Query‑sort** (`"sort":[{"@timestamp":"asc"}]`) | No re‑index | • CPU/heap each run<br>• Large scroll pages slower | Inside `"source"` |

If neither is used, Lucene gives docs in *segment order* (basically random).

---

<a name="9"></a>
## 9 How to interrogate the roll‑up index

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

<a name="10"></a>
## 10 Performance hints & pitfalls

* **Shard size** — smaller shards (5–10 GB) finish map‑phase faster.
* **`page_size`** transform setting — raise if you have thousands of docs per user‑day.
* **Geo lookup** — do in ingest pipeline, *not* in map‑script (which has no IP).
* **Script Sandboxing** — Painless runs on data nodes; infinite loops = crashed task.

---

<a name="11"></a>
## 11 Mini FAQ

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

<a name="12"></a>
## 12 Resources

- Painless API Reference
https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference.html

- "Painless API Reference | Painless Scripting Language [8.17] - Elastic" https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference.html?utm_source=chatgpt.com

- "Shared API for package java.text | Painless Scripting Language [8.17]" https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-text.html?utm_source=chatgpt.com

- "Scripted metric aggregation | Elasticsearch Guide [8.18] | Elastic"
 https://www.elastic.co/guide/en/elasticsearch/reference/8.18/search-aggregations-metrics-scripted-metric-aggregation.html?utm_source=chatgpt.com

- "Shared API for package java.lang | Painless Scripting Language [8.18]" https://www.elastic.co/guide/en/elasticsearch/painless/8.18/painless-api-reference-shared-java-lang.html?utm_source=chatgpt.com

- ``doc['field']`` access & value/size() helpers
https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-fields.html 
elastic.co

- ``java.lang.Math`` & other allowed classes
https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference.html → Shared API → java.lang 
elastic.co

