-- charges_daily — every Charge created after the cursor floor.
--
-- Sigma's dialect is PrestoDB-flavored. The {since} placeholder is
-- substituted by source.py at run time from the engine's Cursor; it
-- arrives here as an ISO-8601 string and is compared against `created`
-- (a Sigma TIMESTAMP column).
--
-- Schema is mirrored in register.yaml. If you add a column here, add
-- it there (and bump the Stripe Sigma API version pin if the new
-- column is preview-only).

SELECT
    id,
    amount,
    currency,
    created,
    status,
    customer_id,
    description,
    paid,
    refunded
FROM charges
WHERE created > TIMESTAMP {since}
ORDER BY created
