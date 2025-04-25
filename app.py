import streamlit as st
import re
import yaml
import pandas as pd
import databricks.sql as dbsql

# --------------------------
# Databricks SQL connection config
hostname     = ""
http_path    = ""
access_token = ""

def run_query(query: str) -> pd.DataFrame:
    with dbsql.connect(server_hostname=hostname, http_path=http_path, access_token=access_token) as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

def execute_query(query: str):
    with dbsql.connect(server_hostname=hostname, http_path=http_path, access_token=access_token) as conn:
        cur = conn.cursor()
        cur.execute(query)

# --------------------------
# DAX Validation Logic
DAX_FUNCTION_SIGNATURES = {
    "SUM": (1, 1),
    "COUNT": (1, 1),
    "AVERAGE": (1, 1),
    "MIN": (1, 1),
    "MAX": (1, 1),
    "CALCULATE": (1, None),
    "COUNTROWS": (1, 1),
    "FILTER": (2, 2),
    "DISTINCTCOUNT": (1, 1)
}

def split_arguments(arg_str: str) -> list:
    args = []
    current = ""
    depth = 0
    for char in arg_str:
        if char == "," and depth == 0:
            args.append(current.strip())
            current = ""
        else:
            current += char
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
    if current:
        args.append(current.strip())
    return args

def validate_dax_recursive(dax_expr: str) -> (bool, str):
    dax_expr = dax_expr.strip()
    match = re.match(r'^([A-Z]+)\s*\((.*)\)$', dax_expr, re.IGNORECASE)
    if not match:
        return False, f"Invalid DAX expression format: '{dax_expr}'"

    func_name = match.group(1).upper()
    arg_str = match.group(2).strip()

    if func_name not in DAX_FUNCTION_SIGNATURES:
        return False, f"Unsupported function '{func_name}'"

    try:
        args = split_arguments(arg_str)
    except Exception:
        return False, "Error parsing arguments (check parentheses)."

    min_args, max_args = DAX_FUNCTION_SIGNATURES[func_name]
    if len(args) < min_args:
        return False, f"{func_name} requires at least {min_args} argument(s)"
    if max_args is not None and len(args) > max_args:
        return False, f"{func_name} accepts at most {max_args} argument(s); got {len(args)}"

    for arg in args:
        if re.match(r'^[A-Z]+\s*\(.*\)$', arg, re.IGNORECASE):
            valid, msg = validate_dax_recursive(arg)
            if not valid:
                return False, msg
        elif "[" in arg and "]" in arg:
            if not re.match(r'^\w+\[\w+\]$', arg.strip()):
                return False, f"Invalid column reference: '{arg.strip()}'"

    return True, ""

def validate_dax(dax_expr: str) -> bool:
    valid, msg = validate_dax_recursive(dax_expr)
    if not valid:
        st.error(f"❌ DAX Validation Error: {msg}")
    return valid

def dax_to_uc_metric_yaml(dax_expr: str, measure_name: str, source: str) -> str:
    try:
        if not validate_dax(dax_expr):
            raise ValueError("Invalid DAX Syntax")

        simplified_expr = dax_expr.strip()
        if simplified_expr.upper().startswith("CALCULATE"):
            m = re.match(r'CALCULATE\s*\(\s*(.+)\)$', simplified_expr, re.IGNORECASE)
            if m:
                simplified_expr = m.group(1).strip()
            else:
                raise ValueError("Syntax error at or near end of input: missing ')'.")
        simplified_expr = re.sub(r'(?i)\bAVERAGE\b', 'AVG', simplified_expr)
        simplified_expr = re.sub(r'(?i)\bDISTINCTCOUNT\s*\(', 'COUNT(DISTINCT ', simplified_expr)
        tm = re.search(r'(\w+)\[([\w]+)\]', simplified_expr)
        if tm:
            sql = re.sub(r'\b\w+\[([\w]+)\]', r'\1', simplified_expr)
        else:
            tm2 = re.search(r'COUNTROWS\s*\(\s*(\w+)\s*\)', simplified_expr, re.IGNORECASE)
            if tm2:
                sql = "COUNT(1)"
            else:
                raise ValueError("Not able to Convert to YAML")

        yd = {
            "version": "1.0",
            "source": source,
            "dimensions": [],
            "measures": [
                {"name": measure_name, "expr": sql, "type": "int"}
            ]
        }
        return yaml.dump(yd, sort_keys=False).strip()
    except Exception as e:
        raise ValueError(f"{e}")

# --------------------------
st.title("DAX → Unity Catalog Metric YAML")

# Sidebar: Source Table
st.sidebar.header("UC DAX Table")
catalog = st.sidebar.text_input("Catalog", "powerbi_uc")
schema  = st.sidebar.text_input("Schema", "uc_metrics")
table   = st.sidebar.text_input("Table", "")
if st.sidebar.button("Submit"):
    input_table = f"{catalog}.{schema}.{table}"
    try:
        df = run_query(f"SELECT * FROM {input_table}")
        st.session_state.df = df
        st.session_state.source = input_table
        st.session_state.loaded = True
        st.success(f"✅ Loaded {len(df)} rows from {input_table}")
    except Exception as e:
        st.error(f"❌ Could not read {input_table}: {e}")
        st.session_state.loaded = False

# Show Measures
if st.session_state.get("loaded"):
    st.subheader("📋 Current DAX Measures")
    st.dataframe(st.session_state.df)

    choice = st.radio("Do you want to add another DAX measure?", ["No", "Yes"])
    if choice == "Yes":
        st.subheader("➕ Add New DAX Measure")
        with st.form("add_form"):
            new_id   = st.text_input("Id (optional)")
            new_dax  = st.text_area("DAX Expression")
            new_name = st.text_input("Measure Name")
            add      = st.form_submit_button("Add Measure")

            if add:
                if not new_dax or not new_name:
                    st.error("Provide both DAX and Measure Name")
                elif not validate_dax(new_dax):
                    st.error("Invalid DAX Syntax")
                else:
                    escaped_dax  = new_dax.replace("'", "''")
                    escaped_name = new_name.replace("'", "''")

                    cols = []
                    vals = []
                    if new_id:
                        cols.append("Id")
                        vals.append(f"'{new_id}'")
                    cols += ["DAX", "Measure"]
                    vals += [f"'{escaped_dax}'", f"'{escaped_name}'"]

                    sql_ins = (
                        f"INSERT INTO {st.session_state.source} "
                        f"({', '.join(cols)}) VALUES ({', '.join(vals)})"
                    )
                    try:
                        execute_query(sql_ins)
                        st.success("✅ Inserted new measure!")
                        df = run_query(f"SELECT * FROM {st.session_state.source}")
                        st.session_state.df = df
                        st.dataframe(df)
                    except Exception as e:
                        st.error(f"Insert failed: {e}")

    # Target for Metric View
    st.subheader("📦 Target Metric View Table")
    target = st.text_input("Enter catalog.schema.tablename .  For MVP plese just use powerbi_uc.uc_metrics.orders ", key="target_table")

    # Convert to YAML
    st.subheader("🛠️ Generate & Deploy")
    if st.button("Convert All Measures to YAML"):
        if not target:
            st.error("Please provide the target view name.")
        else:
            all_measures = []
            failed_rows = []
            for _, row in st.session_state.df.iterrows():
                dax_expr = row["DAX"]
                measure_name = row["Measure"]
                row_id = row.get("Id", "?")

                try:
                    single = dax_to_uc_metric_yaml(dax_expr, measure_name, target)
                    if single.startswith("version"):
                        parsed = yaml.safe_load(single)
                        all_measures.append(parsed["measures"][0])
                    else:
                        raise ValueError(single)
                except Exception as e:
                    failed_rows.append((dax_expr, str(e)))
                    st.warning(f"⚠️ Skipped row {row_id} ('{dax_expr}') → Error: {e}")

            if not all_measures:
                st.error("❌ No valid measures could be converted.")
            else:
                combined = {
                    "version": "1.0",
                    "source": target,
                    "dimensions": [],
                    "measures": all_measures
                }
                combined_yaml = yaml.dump(combined, sort_keys=False)

                st.code(combined_yaml, language="yaml")
                st.download_button("📥 Download YAML", combined_yaml, file_name="uc_metric_view.yml", mime="text/yaml")

                fqn = "`" + "`.`".join(target.split(".")) + "_MetricView`"
                create_sql = f"""CREATE OR REPLACE VIEW {fqn}
WITH METRICS
LANGUAGE YAML
AS $$
{combined_yaml}
$$
"""
                try:
                    execute_query(create_sql)
                    st.success(f"✅ View deployed as {fqn}")
                except Exception as e:
                    st.error(f"Failed to deploy view: {e}")

            # Summary of failed conversions
            if failed_rows:
                st.subheader("🚫 Skipped Measures")
                for expr, err in failed_rows:
                    st.error(f"❌ Not able to convert `{expr}` → Reason: {err}")
