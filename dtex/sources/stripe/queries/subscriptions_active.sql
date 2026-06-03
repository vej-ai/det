-- subscriptions_active — currently-active subscriptions, full snapshot.
--
-- No {since} placeholder: this stream is full-refresh (`write_disposition:
-- replace` in register.yaml). Sigma's `subscriptions` table is the
-- authoritative current state of every subscription.

SELECT
    id,
    customer_id,
    status,
    current_period_start,
    current_period_end,
    cancel_at_period_end,
    created
FROM subscriptions
WHERE status = 'active'
ORDER BY created
