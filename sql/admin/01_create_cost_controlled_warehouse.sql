-- Cost-controlled project warehouse (X-Small, auto-suspend 60s).
-- Guardrails only — no data load.

CREATE OR REPLACE WAREHOUSE DE_PROJECT_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Cost-controlled X-Small warehouse for ECommerceStream-Lakehouse';

-- INITIALLY_SUSPENDED may already suspend a new warehouse; ignore duplicate suspend.
!set exit_on_error=false
ALTER WAREHOUSE DE_PROJECT_WH SUSPEND;
!set exit_on_error=true
