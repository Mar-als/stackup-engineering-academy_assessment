# Data Quality Report — employees

Generated: 2026-09-16T15:33:13

**Checks run:** 7  •  **Passed:** 3  •  **Failed:** 4

| Check | Status | Details |
|---|---|---|
| completeness | ✅ PASS | employee_id=1.0, full_name=1.0, email=0.99, department=1.0, role=1.0, level=1.0, ... |
| uniqueness | ✅ PASS | 1000 unique / 1000 total |
| validity_numeric | ❌ FAIL | years_experience: 5 value(s) below minimum (0) |
| validity_date | ❌ FAIL | hire_date: 8 unparsable, 0 in the future |
| consistency | ❌ FAIL | years_experience: 5 negative value(s); 3 employee(s) with salary outside their level's band |
| referential_integrity | ✅ PASS | all foreign keys resolve |
| distribution | ❌ FAIL | level: 'Mid' is 37% of non-null values (> 30%); region: 'Abu Dhabi' is 43% of non-null values (> 30%); status: 'Active' is 96% of non-null values (> 30%) |
