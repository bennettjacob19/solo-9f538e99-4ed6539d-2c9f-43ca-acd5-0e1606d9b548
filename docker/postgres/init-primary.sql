-- Primary-side replication setup, executed once by the postgres image
-- entrypoint before the server starts accepting external connections.

-- Dedicated physical-replication role (password kept simple for the drill).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator LOGIN REPLICATION PASSWORD 'replpass';
    END IF;
END
$$;

-- A persistent slot means the primary retains WAL while the standby is down.
SELECT pg_create_physical_replication_slot('standby1_slot')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'standby1_slot'
);
