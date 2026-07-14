-- This works
SELECT *
FROM iceberg_scan(
    '/tmp/iceberg path escaping repro/warehouse/default/triples'
);

-- This fails
SELECT *
FROM iceberg_scan(
    '/tmp/iceberg path escaping repro/warehouse/default/triples',
    allow_moved_paths = true
);
