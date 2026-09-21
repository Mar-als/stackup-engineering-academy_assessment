# Data Quality Report — projects

Generated: 2026-09-16T15:33:13

**Checks run:** 7  •  **Passed:** 5  •  **Failed:** 2

| Check | Status | Details |
|---|---|---|
| completeness | ❌ FAIL | project_id=1.0, project_name=1.0, department=1.0, status=1.0, start_date=0.87, end_date=0.428, ... |
| uniqueness | ✅ PASS | 500 unique / 500 total |
| validity_numeric | ✅ PASS | all values within range |
| validity_date | ✅ PASS | all dates valid |
| consistency | ✅ PASS | all consistency rules satisfied |
| referential_integrity | ✅ PASS | all foreign keys resolve |
| distribution | ❌ FAIL | status: 'Completed' is 43% of non-null values (> 30%); priority: 'Medium' is 43% of non-null values (> 30%); region: 'Abu Dhabi' is 44% of non-null values (> 30%) |
