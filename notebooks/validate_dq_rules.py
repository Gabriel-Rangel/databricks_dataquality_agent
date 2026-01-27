# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Validation
# MAGIC Validate data quality rules against a table and return summary results.
# MAGIC
# MAGIC **Note:** The `databricks-labs-dqx` library is installed via job environment dependencies.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("rules", "[]", "Rules JSON")

table_name = dbutils.widgets.get("table_name")
rules_json = dbutils.widgets.get("rules")

# COMMAND ----------

import json
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

# Parse rules from JSON
try:
    rules = json.loads(rules_json)
except json.JSONDecodeError as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Invalid rules JSON: {str(e)}"
    }))

if not rules:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": "No rules provided"
    }))

print(f"Validating {len(rules)} rules against table: {table_name}")

# COMMAND ----------

# Load the table data
try:
    df = spark.table(table_name)
    total_rows = df.count()
    print(f"Loaded {total_rows} rows from {table_name}")
except Exception as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Failed to load table: {str(e)}"
    }))

# COMMAND ----------

# Initialize DQ Engine and apply checks
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# Apply checks and split into valid/invalid DataFrames
try:
    valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(df, rules)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"Valid rows: {valid_count}")
    print(f"Invalid rows: {invalid_count}")
except Exception as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Failed to apply checks: {str(e)}"
    }))

# COMMAND ----------

# Analyze rule-by-rule results
# Apply checks to get the annotated DataFrame with _error and _warning columns
checked_df = dq_engine.apply_checks_by_metadata(df, rules)

# Debug: show schema of checked_df to understand _error/_warning structure
print("Checked DataFrame columns:", checked_df.columns)

# Collect rule-level statistics
rule_results = []

for idx, rule in enumerate(rules):
    check_info = rule.get("check", {})
    func_name = check_info.get("function", "unknown")
    args = check_info.get("arguments", {})
    criticality = rule.get("criticality", "error")
    rule_name = rule.get("name", func_name)  # Use explicit name if provided

    # Get column name from various possible field names
    column = args.get("column") or args.get("col_name") or \
             (args.get("columns", [None])[0] if args.get("columns") else None) or \
             (args.get("col_names", [None])[0] if args.get("col_names") else None) or "-"

    # Count violations for this specific rule by checking _error/_warning columns
    violation_count = 0
    status = "pass"

    try:
        # DQX uses plural column names: _errors and _warnings
        target_col = "_errors" if criticality.lower() == "error" else "_warnings"

        if target_col in checked_df.columns:
            # DQX stores violations as array of structs - check if array is not empty
            # and contains our rule (by function name or rule name)
            from pyspark.sql.functions import col, size, expr

            # First try: count rows where the target column array is not empty
            # This gives us violations for ALL rules in this criticality level
            violations_df = checked_df.filter(size(col(target_col)) > 0)

            # Try to filter by specific rule name
            try:
                # Check both the function name and the explicit rule name
                rule_filter = f"exists({target_col}, x -> x.name = '{rule_name}' OR x.name = '{func_name}')"
                specific_violations_df = checked_df.filter(rule_filter)
                violation_count = specific_violations_df.count()
            except Exception as filter_err:
                # Fallback: if only one rule, use the total invalid count
                print(f"Could not filter by rule name, using total: {filter_err}")
                if len(rules) == 1:
                    violation_count = invalid_count
                else:
                    violation_count = violations_df.count()

            print(f"Rule '{rule_name}' ({func_name}) on '{column}': {violation_count} violations")

        if violation_count > 0:
            status = "fail" if criticality.lower() == "error" else "warn"

    except Exception as e:
        # If we can't count individual violations, use overall invalid count for single rule
        print(f"Warning: Could not count violations for {func_name}: {e}")
        if len(rules) == 1:
            violation_count = invalid_count
            if violation_count > 0:
                status = "fail" if criticality.lower() == "error" else "warn"

    rule_results.append({
        "rule_name": func_name,
        "column": str(column),
        "criticality": criticality,
        "status": status,
        "violation_count": violation_count,
        "details": f"Checked {func_name} on column '{column}'"
    })

# COMMAND ----------

# Calculate summary statistics
per_rule_attribution = any(r["violation_count"] > 0 for r in rule_results)

if invalid_count > 0 and not per_rule_attribution:
    print(f"Warning: Could not attribute {invalid_count} violations to specific rules, distributing to all rules")
    for rule_result in rule_results:
        rule_result["violation_count"] = invalid_count
        if rule_result["criticality"].lower() == "error":
            rule_result["status"] = "fail"
        else:
            rule_result["status"] = "warn"

passed = sum(1 for r in rule_results if r["status"] == "pass")
failed = sum(1 for r in rule_results if r["status"] == "fail")
warnings = sum(1 for r in rule_results if r["status"] == "warn")

# Build output
output = {
    "success": True,
    "table_name": table_name,
    "validated_at": datetime.now().isoformat(),
    "total_rules": len(rules),
    "passed": passed,
    "failed": failed,
    "warnings": warnings,
    "total_rows": total_rows,
    "valid_rows": valid_count,
    "invalid_rows": invalid_count,
    "pass_rate": round((valid_count / total_rows * 100), 2) if total_rows > 0 else 0,
    "rule_results": rule_results
}

print(f"\n=== Validation Summary ===")
print(f"Total Rules: {len(rules)}")
print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}")
print(f"Valid Rows: {valid_count}/{total_rows} ({output['pass_rate']}%)")

# COMMAND ----------

# Return result
dbutils.notebook.exit(json.dumps(output, default=str))
