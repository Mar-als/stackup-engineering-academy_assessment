# Data Quality Report — transactions

Generated: 2026-09-16T15:33:13

**Checks run:** 9  •  **Passed:** 5  •  **Failed:** 4

| Check | Status | Details |
|---|---|---|
| completeness | ❌ FAIL | transaction_id=1.0, project_id=1.0, vendor_id=1.0, vendor_name=1.0, category=1.0, amount=0.9852, ... |
| uniqueness | ✅ PASS | 50000 unique / 50000 total |
| validity_numeric | ✅ PASS | all values within range |
| validity_date | ✅ PASS | all dates valid |
| consistency | ✅ PASS | all consistency rules satisfied |
| referential_integrity | ✅ PASS | all foreign keys resolve |
| distribution | ❌ FAIL | currency: 'AED' is 100% of non-null values (> 30%); payment_status: 'Paid' is 75% of non-null values (> 30%) |
| freshness | ❌ FAIL | most recent transaction_date is 43 day(s) old (threshold 30) |
| outliers | ❌ FAIL | amount: 1593 value(s) beyond 3.0 std dev from the mean |
