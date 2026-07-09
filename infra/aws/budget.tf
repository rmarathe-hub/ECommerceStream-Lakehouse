# Monthly AWS cost budget with email alerts (cost guardrail).
# Requires at least one email in budget_alert_emails when create_budget_alert is true.

resource "aws_budgets_budget" "lakehouse" {
  count = var.create_budget_alert && length(var.budget_alert_emails) > 0 ? 1 : 0

  name         = "${var.project_name}-${var.environment}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_monthly_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = var.budget_alert_thresholds
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.budget_alert_emails
    }
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-${var.environment}-budget"
  })
}
