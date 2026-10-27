-- LOCAL DEVELOPMENT ONLY: credentials are intentionally simple and checked in.
--
-- Database-per-service on a shared instance: every service gets its own login role and
-- its own database. A role owns only its database, and CONNECT is revoked from PUBLIC,
-- so a service cannot even open a connection to another service's database.
-- CONNECTION LIMIT is a second bulkhead behind the per-database PgBouncer pools.

CREATE ROLE ordering_svc     LOGIN PASSWORD 'ordering_svc_pwd'     CONNECTION LIMIT 80;
CREATE ROLE invoicing_svc    LOGIN PASSWORD 'invoicing_svc_pwd'    CONNECTION LIMIT 30;
CREATE ROLE notification_svc LOGIN PASSWORD 'notification_svc_pwd' CONNECTION LIMIT 30;

CREATE DATABASE ordering     OWNER ordering_svc;
CREATE DATABASE invoicing    OWNER invoicing_svc;
CREATE DATABASE notification OWNER notification_svc;

REVOKE CONNECT, TEMPORARY ON DATABASE ordering     FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE invoicing    FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE notification FROM PUBLIC;
