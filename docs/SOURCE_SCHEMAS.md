# Source dataset contracts (locked)

Canonical definitions live in `src/skyflow/generator/schemas.py`. This document is the human-readable copy for interviews and downstream modules.

All entities include:

- `created_at` timestamp UTC — source insert time
- `updated_at` timestamp UTC — **primary incremental watermark** (`updated_at >= created_at`)

## Relationships

```
airlines 1──* aircraft
airlines 1──* routes
airlines 1──* flights
airports 1──* routes (origin)
airports 1──* routes (dest)
routes 1──* flights
aircraft 1──* flights
customers 1──* bookings
flights 1──* bookings
bookings 1──* payments
bookings 1──* baggage
customers 1──* customer_feedback
flights 1──* customer_feedback
bookings 1──* customer_feedback
```

Invariant: a flight’s `airline_id` matches both its route’s airline and its aircraft’s owning airline. Origin/destination on `flights` are copied from the route at schedule time (historical correctness if a route record later changes).

## Entities

### airlines
`airline_id` PK, `iata_code` unique, `icao_code`, `airline_name`, `country`, `alliance`, `headquarters_city`, `founded_year`, `status` (active|inactive)

### airports
`airport_id` PK, `iata_code` unique, `icao_code`, `airport_name`, `city`, `country`, `region`, `timezone`, `latitude`, `longitude`

### aircraft
`aircraft_id` PK, `airline_id` FK, `tail_number` unique, `manufacturer`, `model`, `capacity`, `manufacture_year`, `status` (active|maintenance|retired)

### routes
`route_id` PK, `airline_id` FK, `origin_airport_id` FK, `dest_airport_id` FK, `distance_km` (haversine), `typical_duration_min`, `is_international`

### customers
`customer_id` PK, `first_name`, `last_name`, `email` unique, `phone`, `date_of_birth`, `nationality`, `loyalty_tier` (standard|silver|gold|platinum)

Warehouse note: **SCD Type 2** will be applied to `dim_customer` using `loyalty_tier`, `email`, and `phone`. ~12% of generated customers have `updated_at > created_at` to simulate source-system updates.

### flights
`flight_id` PK, `flight_number`, `airline_id` FK, `aircraft_id` FK, `route_id` FK, `origin_airport_id`, `dest_airport_id`, `scheduled_departure_ts`, `scheduled_arrival_ts`, `actual_departure_ts` (null if cancelled), `actual_arrival_ts` (null if cancelled), `status` (arrived|delayed|cancelled), `delay_minutes`, `cancellation_reason`, `distance_km`

### bookings
`booking_id` PK, `booking_ref` unique PNR, `customer_id` FK, `flight_id` FK, `booking_ts` (before scheduled departure), `cabin_class`, `seat_number`, `fare_amount` (USD; function of distance × cabin × noise), `booking_status`

Seat count per flight ≤ aircraft `capacity`. Volume ≈ flights × capacity × `mean_load_factor`.

### payments
`payment_id` PK, `booking_id` FK (one payment attempt per booking in Module 1), `payment_ts`, `amount`, `currency`, `method`, `status`, `transaction_ref`

### baggage
`baggage_id` PK, `booking_id` FK, `tag_number`, `piece_count`, `weight_kg`, `status` — sampled from eligible bookings at `baggage_rate`

### customer_feedback
`feedback_id` PK, `customer_id` FK, `flight_id` FK, `booking_id` FK (customer must have a booking on that flight), `rating` 1–5, `nps_score` 0–10, `comments`, `submitted_ts` after arrival

Ratings are correlated with `delay_minutes` (not independent noise).
