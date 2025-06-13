import streamlit as st
import pandas as pd
import yaml
import re
import databricks.sql as dbsql
import random

# --- Databricks SQL config ---
hostname = "adb-984752964297111.11.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/148ccb90800933a1"
access_token = ""

# ---------------------------------
# Function: Run SQL on Databricks
# ---------------------------------
def run_query(query: str) -> pd.DataFrame:
    with dbsql.connect(server_hostname=hostname, http_path=http_path, access_token=access_token) as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

# -----------------------
# DAX → UC SQL Mapping
# -----------------------
DAX_TO_UC_MAP = {
    "SUM": "sum", "AVERAGE": "avg", "AVERAGEX": "avg", "COUNT": "count", "COUNTA": "count",
    "COUNTAX": "count", "COUNTROWS": "count", "MAX": "max", "MIN": "min", "STDEV.P": "stddev_pop",
    "STDEV.S": "stddev_samp", "VAR.P": "var_pop", "VAR.S": "var_samp", "AND": "and", "OR": "or",
    "NOT": "not", "IF": "if", "CONCATENATE": "concat", "LEFT": "left", "RIGHT": "right", "LEN": "length",
    "UPPER": "upper", "LOWER": "lower", "TRIM": "trim", "REPLACE": "replace", "SUBSTITUTE": "replace",
    "NOW": "now", "TODAY": "current_date", "YEAR": "year", "MONTH": "month", "DAY": "day", "HOUR": "hour",
    "MINUTE": "minute", "SECOND": "second", "DATE": "date", "DATEDIFF": "datediff", "ABS": "abs",
    "CEILING": "ceil", "FLOOR": "floor", "ROUND": "round", "POWER": "pow", "EXP": "exp", "LN": "ln",
    "LOG": "log10", "SQRT": "sqrt", "ISBLANK": "isnull", "ISNUMBER": "isnumeric", "ISTEXT": "typeof"
}

def tokenize_dax(dax_expression):
    return re.findall(r'-?\d+\.\d+|-?\d+|[A-Za-z_][A-Za-z0-9_]*|\(|\)|\[|\]|,|\+|\-|\*|\/', dax_expression)

def extract_column(expr):
    match = re.search(r'\[([^\[\]]+)\]', expr)
    return match.group(1).strip() if match else expr.strip()

def extract_source_from_tokens(tokens):
    sources = set()
    for i in range(len(tokens) - 2):
        if re.match(r"[A-Za-z_][A-Za-z0-9_]*", tokens[i]) and tokens[i+1] == "[" and re.match(r"[A-Za-z0-9_]+", tokens[i+2]):
            sources.add(tokens[i])
    return ", ".join(sources)

def extract_supported_expression(dax_expression):
    tokens = tokenize_dax(dax_expression)
    mapped_expr = ""
    window_info = None
    dimension_info = None
    i = 0

    while i < len(tokens):
        token = tokens[i].upper()
        if token == "CALCULATE" and i + 1 < len(tokens) and tokens[i + 1] == "(":
            depth, args, current_arg, j = 1, [], [], i + 2
            while j < len(tokens) and depth > 0:
                if tokens[j] == "(": depth += 1
                elif tokens[j] == ")": depth -= 1
                if tokens[j] == "," and depth == 1:
                    args.append(current_arg); current_arg = []
                else:
                    current_arg.append(tokens[j])
                j += 1
            if current_arg: args.append(current_arg)

            for arg in args:
                if not arg: continue
                func_name = arg[0].upper()
                if func_name in DAX_TO_UC_MAP:
                    func_uc = DAX_TO_UC_MAP[func_name]
                    arg_str = " ".join(arg[1:])
                    col = extract_column(arg_str)
                    mapped_expr = f"{func_uc}({col})"
                elif func_name == "DATEADD":
                    try:
                        dax_str = " ".join(arg)
                        match = re.search(r'DATEADD\s*\(\s*(\w+)\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*(\w+)\s*\)', dax_str, re.IGNORECASE)
                        if match:
                            table = match.group(1)
                            column = match.group(2)
                            number = int(match.group(3))
                            unit = match.group(4).upper()
                            range_type = "trailing" if number < 0 else "leading"
                            dim_name = f"{table}_Date"
                            dimension_info = {
                                "name": dim_name,
                                "expr": f"date_trunc('{unit}', {column})",
                                "type":"int"
                            }
                            window_info = {
                                "order": dim_name,
                                "range": f"{range_type} {abs(number)} {unit}",
                                "semiadditive": "last"
                            }
                    except Exception as e:
                        print(f"DATEADD parse error: {e}")
            return mapped_expr, True, tokens, dimension_info, window_info

        elif token in DAX_TO_UC_MAP:
            func = DAX_TO_UC_MAP[token]
            if i + 1 < len(tokens) and tokens[i + 1] == "(":
                depth, args, j = 1, [], i + 2
                while j < len(tokens) and depth > 0:
                    if tokens[j] == "(": depth += 1
                    elif tokens[j] == ")": depth -= 1
                    elif tokens[j] == "[" and j + 1 < len(tokens):
                        args.append(tokens[j + 1])
                        j += 1
                    j += 1
                mapped_expr = f"{func}({', '.join(args)})"
                return mapped_expr, True, tokens, None, None
        i += 1
    return "", False, tokens, None, None

def generate_combined_yaml(source, dimensions, measures):
    return {
        "version": 0.1,
        "source": source,
        "dimensions": dimensions,
        "measures": measures
    }

# ---------------- UI Starts ---------------------
st.set_page_config(page_title="DAX to Unity Catalog Metrics YAML Converter", layout="wide")

# Header & intro
st.title("🔁 Power BI DAX → Unity Catalog Metrics YAML")
st.markdown(
    """
    This tool converts your Power BI DAX measures into Unity Catalog Metric Views YAML format.
    Follow the steps below:
    """
)

# Session state defaults
if "show_generate_yaml" not in st.session_state:
    st.session_state["show_generate_yaml"] = False
if "source_table" not in st.session_state:
    st.session_state["source_table"] = ""
if "dax_input_enabled" not in st.session_state:
    st.session_state["dax_input_enabled"] = False

# --- Step 1: Table info input ---
with st.expander("Step 1: Enter source table details", expanded=True):
    with st.form("table_form", clear_on_submit=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            catalog = st.text_input("Catalog", value=st.session_state.get("catalog", ""), help="Databricks Unity Catalog name")
        with col2:
            schema = st.text_input("Schema", value=st.session_state.get("schema", ""), help="Schema name")
        with col3:
            table = st.text_input("Table", value="", help="Table name")

        submitted = st.form_submit_button("🔍 Validate Table")

    if submitted:
        if not (catalog and schema and table):
            st.warning("Please fill in Catalog, Schema, and Table before submitting.")
        else:
            source = f"{catalog}.{schema}.{table}"
            try:
                df_preview = run_query(f"SELECT * FROM {source} LIMIT 5")
                st.success(f"✅ Successfully connected to table: {source}")
                st.dataframe(df_preview, use_container_width=True)
                st.session_state["source_table"] = source
                st.session_state["dax_input_enabled"] = True
                st.session_state["catalog"] = catalog
                st.session_state["schema"] = schema
            except Exception as e:
                st.error(f"Failed to query table: {e}")
                st.session_state["dax_input_enabled"] = False

# --- Step 2: DAX measures input ---
if st.session_state.get("dax_input_enabled"):
    with st.expander("Step 2: Enter DAX Measures"):
        want_dax = st.radio("Do you want to enter DAX measures?", ["Yes", "No"], horizontal=True)
        if want_dax == "Yes":
            dax_input = st.text_area(
                "Enter DAX measures below (one per line, format: measure_name: DAX_expression)",
                height=250,
                placeholder="e.g. total_sales: CALCULATE(SUM(Sales[Amount]))"
            )

            if st.button("📥 Insert DAX into Table") and dax_input.strip():
                try:
                    # Create table if doesn't exist (Note: Your original table is used here)
                    create_stmt = f"""CREATE TABLE IF NOT EXISTS {st.session_state['source_table']} (
                        id INT, measure STRING, dax STRING)"""
                    run_query(create_stmt)

                    # Insert measures
                    for line in dax_input.strip().split('\n'):
                        if ':' not in line:
                            continue
                        measure, dax_expr = map(str.strip, line.split(':', 1))
                        rand_id = random.randint(10000, 99999)
                        insert_stmt = f"""INSERT INTO {st.session_state['source_table']} VALUES
                            ({rand_id}, '{dax_expr.replace("'", "''")}', '{measure.replace("'", "''")}')"""
                        run_query(insert_stmt)

                    st.success("✅ DAX measures inserted successfully.")
                    st.dataframe(run_query(f"SELECT * FROM {st.session_state['source_table']}"), use_container_width=True)
                    st.session_state["show_generate_yaml"] = True

                except Exception as e:
                    st.error(f"Insertion failed: {e}")

        else:
            st.session_state["show_generate_yaml"] = True

# --- Step 3: Generate YAML and create view ---
if st.session_state.get("show_generate_yaml"):
    with st.expander("Step 3: Generate YAML and Create Metric View", expanded=True):
        uc_catalog = st.text_input("Destination Catalog for Metric View", st.session_state.get("catalog", ""))
        uc_schema = st.text_input("Destination Schema for Metric View", st.session_state.get("schema", ""))
        metric_view_name = st.text_input("Metric View Name", value="my_metric_view")

        if st.button("🚀 Generate YAML"):
            if not (uc_catalog and uc_schema and metric_view_name):
                st.warning("Please enter Catalog, Schema, and Metric View Name.")
            else:
                try:
                    df_dax = run_query(f"SELECT measure, dax FROM {st.session_state['source_table']}")
                    if df_dax.empty:
                        st.warning("No DAX expressions found in the table.")
                    else:
                        status_table, measures_list, dimension_set, full_sources_set = [], [], set(), set()

                        for _, row in df_dax.iterrows():
                            measure_name, dax_expr = row["measure"], row["dax"]
                            mapped_expr, supported, tokens, dimension_info, window_info = extract_supported_expression(dax_expr)
                            source_table = extract_source_from_tokens(tokens)
                            yaml_source = source_table.split(",")[0] if "," in source_table else source_table
                            full_sources_set.add(f"{uc_catalog}.{uc_schema}.{yaml_source}")

                            measure_yaml = {"name": measure_name, "expr": mapped_expr}
                            if supported and mapped_expr:
                                if window_info: measure_yaml["window"] = [window_info]
                                if dimension_info: dimension_set.add(tuple(dimension_info.items()))
                                measures_list.append(measure_yaml)
                                status = "Converted"
                            else:
                                status = "Not Converted"

                            status_table.append({
                                "Measure Name": measure_name,
                                "DAX": dax_expr,
                                "Tokens": ', '.join(tokens),
                                "Mapped Expression": mapped_expr or "[NOT_SUPPORTED]",
                                "Source": yaml_source,
                                "Status": status
                            })

                        st.subheader("📋 Conversion Status")
                        st.dataframe(pd.DataFrame(status_table), use_container_width=True)

                        if measures_list:
                            st.subheader("🧾 Combined Unity Catalog Metrics YAML")
                            selected_source = list(full_sources_set)[0]
                            dimensions_list = [{"name": d["name"], "expr": d["expr"]} for d in [dict(pairs) for pairs in dimension_set]]
                            combined_yaml = generate_combined_yaml(selected_source, dimensions_list, measures_list)
                            yaml_text = yaml.dump(combined_yaml, sort_keys=False)
                            st.code(yaml_text, language="yaml")

                            # Save YAML and info in session state for Create View button
                            st.session_state["yaml_text"] = yaml_text
                            st.session_state["selected_source"] = selected_source
                            st.session_state["metric_view_name"] = metric_view_name
                        else:
                            st.info("No convertible expressions found.")

                except Exception as e:
                    st.error(f"Failed to read DAX measures from table: {e}")

        # Create View Button
        if all(key in st.session_state for key in ["yaml_text", "selected_source", "metric_view_name"]):
            if st.button("✅ Create Unity Catalog Metrics View"):
                try:
                    create_view_sql = f"""
CREATE or replace VIEW {uc_catalog}.{uc_schema}.{st.session_state['metric_view_name']}
WITH METRICS
LANGUAGE YAML AS $$
{st.session_state['yaml_text']}
$$
"""
                    run_query(create_view_sql)
                    st.success(f"Metric View '{uc_catalog}.{uc_schema}.{st.session_state['metric_view_name']}' created successfully!")
                except Exception as e:
                    st.error(f"Failed to create metric view: {e}")
