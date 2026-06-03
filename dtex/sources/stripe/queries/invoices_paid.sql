-- invoices_paid — paid invoices since the cursor floor.
--
-- Sigma's invoices table tracks lifecycle transitions in the
-- `status_transitions_*` columns. `status_transitions_paid_at` is the
-- timestamp the invoice transitioned to paid; that's the right
-- incremental cursor (`created` or `date` would re-load an invoice
-- whose payment processed long after it was issued).

SELECT
    id,
    customer_id,
    subscription_id,
    total,
    amount_paid,
    currency,
    status_transitions_paid_at,
    status,
    number
FROM invoices
WHERE status_transitions_paid_at > TIMESTAMP {since}
ORDER BY status_transitions_paid_at
