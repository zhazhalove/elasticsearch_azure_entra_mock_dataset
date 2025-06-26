**walk through the exact flow using the same scenario:**

- **User:** alice@example.com
- **Day:** 2025-06-25
- **Sign-ins:** 5 total documents.
- **Shard Distribution:**
  - **Shard A** holds 3 of Alice's documents.
  - **Shard B** holds the other 2.
- **The Goal:** Find the absolute maximum speed, even if it's between the last sign-in on Shard A and the first sign-in on Shard B.

---

**Phase 1: Intra-Shard Analysis (Within each shard)**

**Step 1 & 2: Collection (``init_script`` & ``map_script``)**

This part is the same as before. The transform runs in parallel on all shards.

- **On Shard A:** A list state.locations is created and populated with 3 of Alice's location data points (timestamp, lat, lon) in no particular order.

- **On Shard B:** A separate list state.locations is created and populated with the other 2 location data points.

```

                   COORDINATOR NODE
                        |
      +-----------------+-----------------+
      |                 |                 |
   SHARD A           SHARD B           SHARD C
(has 3 docs)      (has 2 docs)       (has 0 docs)
---------------   ---------------   ---------------
state.locations   state.locations   (nothing happens
= [loc1, loc2, loc3] = [loc4, loc5]  for Alice)
 (unsorted)        (unsorted)

```

**Step 3: Pre-Processing & Packaging (``combine_script``)**
This is where the new logic begins. The ``combine_script`` runs on each shard that has data for Alice's group. Its job is to perform a full analysis of its local data and package it up for the final reduction.

- **On Shard A:**

1. The script receives the list of 3 unsorted locations.
2. It sorts this list chronologically.
3. It calculates the maximum speed found only between these 3 points. Let's say the highest speed found here is 850 km/h.
4. It **packages** its findings into a map and returns it. This map contains the intra-shard max speed, and the chronologically first and last data points it knows about.
   - **Returns:** ``{ max_speed: 850.0, first: {ts:..., lat:..., lon:...}, last: {ts:..., lat:..., lon:...} }``

**On Shard B:**

1. The script sorts its list of 2 locations.
2. It calculates the max speed between these two points. Let's say it's only 50 km/h.
3. It packages its findings into its own map.
   - **Returns:** ``{ max_speed: 50.0, first: {ts:..., lat:..., lon:...}, last: {ts:..., lat:..., lon:...} }``

Now, the shards send these compact summary packages to the coordinator node.
```
                  COORDINATOR NODE
     (receives summary package from each shard)
                        |
      +-----------------+-----------------+
      |                 |                 |
   SHARD A           SHARD B           SHARD C
---------------   ---------------   ---------------
Returns map:      Returns map:      ...
{max: 850,...}    {max: 50,...}
```

---

**Phase 2: Inter-Shard Analysis (On the Coordinator Node)**

**Step 4: Final Reduction (``reduce_script``)**

The coordinator node now has everything it needs to perform the final, complete calculation. It executes the reduce_script.

1. **Gather & Sort:** The script receives the two summary maps in a list called ``states``.

   - ``states`` = ``[ {max_speed: 850, first:..., last:...}, {max_speed: 50, first:..., last:...} ]``

   - **Crucially, it first sorts this ``states`` list.** It compares the timestamp of the ``first`` point in each map. This ensures that the shard with the earliest data (Shard A) is processed before the shard with later data (Shard B).

   - Sorted ``states`` = ``[ {Shard A's map}, {Shard B's map} ]``

2. **Iterate and Calculate:** The script loops through the sorted list of shard summaries.

   - Initialize: globalMax = 0.0, lastPointFromPreviousShard = null.

    - **Processing Shard A's map:**

      - The script checks Shard A's pre-calculated speed. ``850.0`` is greater than ``globalMax (0.0)``, so ``globalMax`` becomes ``850.0``.

       - It then checks if it needs to do a cross-shard calculation. ``lastPointFromPreviousShard`` is ``null``, so it skips this.

       - It sets ``lastPointFromPreviousShard`` to the ``last`` point from Shard A's map, saving it for the next step.

    - **Processing Shard B's map:**

      - The script checks Shard B's speed. ``50.0`` is not greater than ``globalMax`` (850.0), so ``globalMax`` remains ``850.0``.

      - It checks for a cross-shard calculation. ``lastPointFromPreviousShard`` is **not** null! It holds the last known location from Shard A.

      - The script now performs the **cross-shard boundary calculation:** it computes the speed between ``lastPointFromPreviousShard`` and the ``first`` point from Shard B's map.

      - Let's say this is the actual impossible travel event, and the speed is ``12,000 km/h.``

      - This new speed (``12,000``) is greater than ``globalMax`` (``850``), so ``globalMax`` is updated to ``12,000.0``.

      - It updates ``lastPointFromPreviousShard`` to the ``last`` point from Shard B's map.

3. **Final Result:** The loop finishes. The script returns the final ``globalMax`` value of ``12000.0``.

This final result is then written to the ``entra_impossible_travel_daily`` index. This two-phase process guarantees that the true maximum speed is found, whether it occurs neatly within one shard's data or across the boundaries of two or more shards.