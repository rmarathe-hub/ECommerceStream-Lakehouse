-- Explicit suspend after any Snowflake session (cost safety).

-- Explicit suspend after sessions (idempotent when already suspended).
!set exit_on_error=false
ALTER WAREHOUSE DE_PROJECT_WH SUSPEND;
!set exit_on_error=true
