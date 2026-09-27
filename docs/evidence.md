# Evidence from the pipeline

## January 2024 live run

I ran the live pipeline using the official January 2024 Yellow Taxi PARQUET file and the Taxi Zones JSON API.

| Check or metric | Result |
| --- | ---: |
| Raw trip rows read | 2,964,624 |
| Accepted trip rows | 2,931,068 |
| Acceptance rate | 98.8681% |
| P50 journey duration | 11.6 minutes |
| P90 journey duration | 28.6 minutes |
| Long-trip rate (>45 minutes) | 2.8959% |
| Largest pickup-zone share of trips | 4.8741% |

The downloaded trip file had this SHA-256 hash:

```text
c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510
```

### Validation results

33,556 rows were rejected (1.1319%). The most common issues were pickup/drop-off IDs that were not in the current geographic zone reference, a journey over 240 minutes, and a non-positive journey duration. The pipeline keeps the rejected raw rows and reason codes instead of changing them.

### What I would do with this result

The citywide result is a baseline, not a recommendation by itself. I would open `pickup_zone_metrics.csv`, filter out very small groups, and compare zones with a high P90 or long-trip rate to the citywide numbers. Those zones would be candidates for a closer operational investigation.

## Small fixture run

The repository also has a 20-row synthetic fixture. It is there so the project can be run offline and so the validation behaviour is easy to see. It is **not** New York City evidence.

| Check or metric | Fixture result |
| --- | ---: |
| Raw rows | 20 |
| Accepted rows | 18 |
| P50 duration | 25.0 minutes |
| P90 duration | 53.0 minutes |
| Long-trip rate | 16.67% |

The two rejected fixture rows show the two validation cases I wanted to demonstrate: a trip longer than 240 minutes and a pickup LocationID that does not exist in the reference file. One accepted row has a negative total amount, which is recorded as a warning rather than being silently removed.
