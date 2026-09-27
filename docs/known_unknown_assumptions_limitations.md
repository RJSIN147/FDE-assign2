# Known, unknown, assumptions and limitations

## What I know from the sources

- A Yellow Taxi record has a pickup timestamp, drop-off timestamp, pickup/drop-off LocationIDs, trip distance and fare fields.
- The Taxi Zones source gives names and boroughs for geographic LocationIDs.
- The pipeline reports how many rows were accepted and rejected each time it runs.

## What I do not know

- When a passenger started looking for a taxi or how long they waited.
- Whether a long trip was caused by traffic, the route, a queue, weather, a passenger request, or normal airport travel.
- Whether the whole taxi market behaves the same way. This is only Yellow Taxi data.

## Assumptions I made

- `tpep_pickup_datetime` is the time the meter started and `tpep_dropoff_datetime` is when it stopped, following the TLC data dictionary.
- A duration above 0 and at most 240 minutes is reasonable enough to include in this monthly analysis. The 240-minute cutoff is a data-quality rule, not a claim that a longer ride is impossible.
- A trip over 45 minutes is a useful flag for investigation. It is not automatically a bad trip.
- For a zone-level decision, both LocationIDs need to join to the zone source.

## Limitations

- This cannot measure customer wait time, acceptance rate, cancellation rate, driver supply, or route quality.
- The current Taxi Zones API has 263 geographic zones. Trips with an ID not found there are saved as rejected rows instead of being assigned a made-up zone. This affects the coverage number, which is why I report it.
- Airport and long-distance trips can naturally be longer than local trips. A high rate alone is not a causal conclusion.
- TLC says the trip records come from technology providers and that it does not guarantee their accuracy. Keeping raw inputs and showing rejected rows helps, but does not remove that limitation.
