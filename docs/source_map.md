# Source map

I started with the question, not with the data file. This is the map I used before writing the pipeline.

| Question I need to answer | Information needed | Source and owner | Grain | How I check the retrieval | Gap I need to remember |
| --- | --- | --- | --- | --- | --- |
| Which pickup areas have longer trips? | Pickup and drop-off timestamps, pickup/drop-off IDs, distance | TLC Yellow Taxi trip records. TLC publishes records sent by its technology providers. | One completed trip | The PARQUET download is saved locally. I record its URL, bytes and SHA-256 hash, and check the PARQUET header/trailer. | There is no request time, dispatch time, route, traffic, or cancellation event. |
| What place does a LocationID refer to? | LocationID, borough and zone name | NYC Open Data Taxi Zones | One zone | I save the API JSON response. The pipeline asks for up to 1,000 rows and expects at least 260, so it will not accidentally use a truncated API response. | A taxi zone is only an approximate neighbourhood. |
| Can I trust the monthly numbers? | Required values, timestamp order, sensible duration, valid zone join and row counts | My validation step | One input row | Every input row is accepted or rejected with a reason. The run fails if over 10% are rejected. | A passed row is usable for this project; it is not proof that every value is perfect. |

## Why two sources?

The trip file gives me IDs such as `161` and `230`, but those are not useful to someone deciding what to look at. The Taxi Zones source turns those IDs into names and boroughs. Because the two sources are separate, I treat the ID join as a check, not as something to assume will always work.

## What is saved during a live run?

For example, a January 2024 run creates `data/raw/month=2024-01/`. It contains the original PARQUET file, the exact zone API response, and `raw_source_manifest.json`. I reuse that folder on later runs unless I deliberately use `--force-retrieve`.
