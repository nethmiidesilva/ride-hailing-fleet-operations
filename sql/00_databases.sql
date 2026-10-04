-- ============================================================================================
-- Runs before init.sql (files in /docker-entrypoint-initdb.d execute in lexical order).
--
-- Airflow keeps its metadata in a SECOND database on the SAME PostgreSQL server rather than in a
-- second container.  Rationale: the whole stack must fit in 8 GB of Docker memory, and a second
-- Postgres instance would cost ~200 MB for no architectural benefit.  Using a separate database
-- (not a schema) still keeps Airflow's ~30 internal tables out of the analytics database, so
-- `\dt` in the fleet database shows only the tables this project designed.
-- ============================================================================================
CREATE DATABASE airflow;
COMMENT ON DATABASE airflow IS 'Airflow 2.9 metadata database (LocalExecutor).';
