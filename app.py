from spark.lineage import get_impacted_views_snowflake, get_impacted_objects_sqlserver
import streamlit as st
from datetime import datetime
import json
import pandas as pd

# -------------------------------
# DB CONNECTORS (DIRECT)
# -------------------------------

from datasources.sqlserver import get_sqlserver_connection
from datasources.snowflake import connect_snowflake
from genai.llm_summary import (
    build_change_analysis_prompt,
    analyze_schema_changes_llm,
    generate_sql_documentation
)
from streamlit.components.v1 import html

# -------------------------------
# ENGINE FUNCTIONS (FROM main)
# -------------------------------

from main import (
    get_tables,
    get_views,
    get_procedures,
    get_table_schema,
    build_dependency_graph,
    render_graph,
    get_baseline_snapshot_sqlite,
    set_baseline,
    persist_snapshot_sqlite,
    build_catalog_snapshot,
    compare_snapshots,
    get_object_timestamps_sqlserver,
    get_object_timestamps_snowflake,
    extract_sql_lineage,
    find_downstream_views,
    find_downstream_procs,
    build_view_html_report,
    save_catalog_to_sqlite
)
from spark.audit_logger import (
    init_app_action_log,
    init_db_change_log,
    log_app_action,
    log_db_change,
)


def build_overview(df, obj_type):
    if df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["object_type"] = obj_type

    # Ensure full_name exists
    if "full_name" not in out.columns:
        out["full_name"] = out["schema"] + "." + out["name"]

    return out

def _build_full_names(df):
    if df is None or df.empty:
        return []

    cols = [c.lower() for c in df.columns]

    df2 = df.copy()
    df2.columns = cols

    if {"schema", "view_name"} <= set(cols):
        return (df2["schema"] + "." + df2["view_name"]).tolist()

    if {"schema_name", "view_name"} <= set(cols):
        return (df2["schema_name"] + "." + df2["view_name"]).tolist()

    if {"table_schema", "table_name"} <= set(cols):
        return (df2["table_schema"] + "." + df2["table_name"]).tolist()

    if "full_name" in cols:
        return df2["full_name"].tolist()

    # fallback — first column
    return df2.iloc[:, 0].astype(str).tolist()

# --------------------------------------------
# PAGE SETUP
# --------------------------------------------
# --------------------------------------------------
# 🧠 Session defaults
# --------------------------------------------------

defaults = {
    "conn": None,
    "db_dialect": None,
    "schema": None,
    "catalog_selected": None,
    "catalog_selected_type": None,
    "catalog_overview": None,
}

for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

if "catalog_selected" not in st.session_state:
    st.session_state["catalog_selected"] = None

if "catalog_selected_type" not in st.session_state:
    st.session_state["catalog_selected_type"] = None


st.set_page_config(page_title="Schema Intelligence", layout="wide")
st.title("🧠 Schema Change Intelligence Platform")

# --------------------------------------------
# SESSION INIT
# --------------------------------------------

for k in [
    "conn",
    "dialect",
    "database",
    "schema",
    "current_snapshot_id",
    "previous_snapshot_id",
    "baseline_auto_set",
]:
    st.session_state.setdefault(k, None)

# ======================================================
# 🔌 CONNECTION UI
# ======================================================

st.subheader("🔌 Database Connection")

dialect = st.selectbox(
    "Select Database",
    ["sqlserver", "snowflake"],
)

if dialect == "sqlserver":

    col1, col2 = st.columns(2)

    with col1:
            server = st.text_input("Server")
            database = st.text_input("Database")
            driver = st.text_input(
                "ODBC Driver",
                "ODBC Driver 17 for SQL Server"
            )

    with col2:
            user = st.text_input("User")
            password = st.text_input(
                "Password",
                type="password"
            )
            schema = st.text_input("Schema")

    if st.button("Connect"):

        conn = get_sqlserver_connection(
            server,
            database,
            user,
            password,
            driver
        )

        st.session_state["conn"] = conn
        st.session_state["dialect"] = "sqlserver"
        st.session_state["database"] = database
        st.session_state["schema"] = schema
        st.session_state.pop("baseline_auto_set", None)

        st.success("✅ Connected to SQL Server")

elif dialect == "snowflake":

    col1, col2 = st.columns(2)

    with col1:
            account = st.text_input(
                "Account (without .snowflakecomputing.com)"
            )
            user = st.text_input("User")
            password = st.text_input(
                "Password",
                type="password"
            )
            
            role = st.text_input("Role (optional)")

    with col2:
            warehouse = st.text_input("Warehouse")
            database = st.text_input("Database")
            schema = st.text_input("Schema")

    if st.button("Connect"):

        conn = connect_snowflake(
            account,
            user,
            password,
            warehouse,
            database,
            schema,
            role,
        )

        st.session_state["conn"] = conn
        st.session_state["dialect"] = "snowflake"
        st.session_state["database"] = database
        st.session_state["schema"] = schema
        st.session_state.pop("baseline_auto_set", None)

        st.success("✅ Connected to Snowflake")

# ======================================================
# 🚀 AFTER CONNECT
# ======================================================

if st.session_state.get("conn"):

    conn = st.session_state["conn"]
    dialect = st.session_state["dialect"]
    database = st.session_state["database"]
    schema = st.session_state["schema"]

    # --------------------------------------------
    # AUTO SCAN ON CONNECT
    # --------------------------------------------

    with st.spinner("🔍 Scanning database catalog..."):

        tables_df = get_tables(conn, dialect, schema, database)
        views_df = get_views(conn, dialect, schema, database)
        procs_df = get_procedures(conn, dialect, schema)

        tables_df.columns = [c.lower() for c in tables_df.columns]
        views_df.columns = [c.lower() for c in views_df.columns]
        procs_df.columns = [c.lower() for c in procs_df.columns]

        tables_df = tables_df.rename(
            columns={
                "table_schema": "schema",
                "table_name": "name",
            }
        )

        views_df = views_df.rename(
            columns={
                "table_schema": "schema",
                "table_name": "name",
            }
        )

        procs_df = procs_df.rename(
            columns={
                "routine_schema": "schema",
                "routine_name": "name",
            }
        )

        # -------------------------------
        # 🧱 BUILD FULL NAMES
        # -------------------------------

        tables_df["full_name"] = tables_df["schema"] + "." + tables_df["name"]
        views_df["full_name"] = views_df["schema"] + "." + views_df["name"]
        procs_df["full_name"] = procs_df["schema"] + "." + procs_df["name"]

        all_columns = []
        # st.write("🧪 tables_df columns:", tables_df.columns.tolist())


        for _, row in tables_df.iterrows():

            df_cols = get_table_schema(
                conn,
                schema,
                row["name"],
                dialect,
                database,
            )
            # -------------------------------
            # 🔁 NORMALIZE COLUMN METADATA
            # -------------------------------

            df_cols.columns = [c.lower() for c in df_cols.columns]

            df_cols = df_cols.rename(
                columns={
                    "column_name": "column_name",   # keep
                    "column": "column_name",
                    "name": "column_name",
                    "data_type": "data_type",
                    "type": "data_type",
                    "is_nullable": "nullable",
                    "nullable": "nullable",
                    "character_maximum_length": "length",
                }
            )


            df_cols["schema"] = schema
            df_cols["object_name"] = row["name"]
            df_cols["object_type"] = "TABLE"


# 👉 REQUIRED for snapshot builder
            df_cols["full_name"] = (
    schema + "." + row["name"]
)


            all_columns.append(df_cols)

        all_columns_df = (
            st.session_state.get("all_columns_df")
            if False
            else (
                json.loads(json.dumps({})) or
                (
                    __import__("pandas").concat(all_columns, ignore_index=True)
                    if all_columns
                    else __import__("pandas").DataFrame()
                )
            )
        )

        if dialect == "sqlserver":
            timestamps_df = get_object_timestamps_sqlserver(conn, schema)
        else:
            timestamps_df = get_object_timestamps_snowflake(
                conn,
                database,
                schema,
            )
        # -------------------------------
        # 🔁 NORMALIZE TIMESTAMPS METADATA (SAFE)
        # -------------------------------

        timestamps_df.columns = [c.lower() for c in timestamps_df.columns]

        # Map possible name columns
        if "name" not in timestamps_df.columns:
            for cand in ["object_name", "table_name", "routine_name"]:
                if cand in timestamps_df.columns:
                    timestamps_df = timestamps_df.rename(columns={cand: "name"})
                    break

        # Map object type
        if "object_type" not in timestamps_df.columns:
            if "type_desc" in timestamps_df.columns:
                timestamps_df = timestamps_df.rename(columns={"type_desc": "object_type"})

        # -------------------------------
        # 🧱 BUILD FULL NAME (only if possible)
        # -------------------------------

        if "schema" in timestamps_df.columns and "name" in timestamps_df.columns:

            timestamps_df["schema"] = timestamps_df["schema"].astype(str)
            timestamps_df["name"] = timestamps_df["name"].astype(str)

            timestamps_df["full_name"] = (
                timestamps_df["schema"] + "." + timestamps_df["name"]
            )

        else:
            st.error(
                f"❌ timestamps_df missing required cols: {timestamps_df.columns.tolist()}"
            )
            st.stop()

        # -------------------------------
        # 🔁 NORMALIZE OBJECT TYPE VALUES
        # -------------------------------

        if "object_type" in timestamps_df.columns:

            timestamps_df["object_type"] = (
                timestamps_df["object_type"]
                .astype(str)
                .str.upper()
            )


        # st.write("🧪 timestamps_df columns:", timestamps_df.columns.tolist())
        # st.write(timestamps_df.head())


        # -------------------------------
        # 🧱 BUILD FULL NAME + TYPE
        # -------------------------------

        timestamps_df["schema"] = timestamps_df["schema"].astype(str)
        timestamps_df["name"] = timestamps_df["name"].astype(str)

        timestamps_df["full_name"] = (
            timestamps_df["schema"] + "." + timestamps_df["name"]
        )

        timestamps_df["object_type"] = (
            timestamps_df["object_type"]
            .astype(str)
            .str.upper()
        )
        # -------------------------------
        # 🎯 MAP ENGINE TYPES → CANONICAL
        # -------------------------------

        TYPE_MAP = {
            "USER_TABLE": "TABLE",
            "BASE TABLE": "TABLE",
            "TABLE": "TABLE",

            "VIEW": "VIEW",

            "SQL_STORED_PROCEDURE": "PROC",
            "STORED_PROCEDURE": "PROC",
            "PROCEDURE": "PROC",
            "PROC": "PROC",
        }

        timestamps_df["object_type"] = (
            timestamps_df["object_type"]
            .map(TYPE_MAP)
            .fillna(timestamps_df["object_type"])
        )

        snapshot = build_catalog_snapshot(
            database=database,
            schema=schema,
            dialect=dialect,
            tables_df=tables_df,
            views_df=views_df,
            procs_df=procs_df,
            columns_df=all_columns_df,
            timestamps_df=timestamps_df,
        )

        snapshot_id = persist_snapshot_sqlite(snapshot)

        st.session_state["current_snapshot_id"] = snapshot_id

    # --------------------------------------------
    # AUTO BASELINE ON FIRST CONNECT
    # --------------------------------------------

    baseline = get_baseline_snapshot_sqlite()

    if not baseline and not st.session_state.get("baseline_auto_set"):

        set_baseline(snapshot_id)

        st.session_state["baseline_auto_set"] = True

        st.success("⭐ Baseline automatically created on connect")

    # --------------------------------------------
    # TABS
    # --------------------------------------------

    tab1, tab2 = st.tabs(["📊 DB Catalog", "🚨 Change Detection"])

    # ======================================================
    # 📊 DB CATALOG
    # ======================================================

    with tab1:

        st.subheader("📦 Current Catalog")

        for t, objs in snapshot["objects"].items():
            st.metric(t, len(objs))

        # ======================================================
        # 📦 CLICKABLE DB CATALOG (PORT FROM main.py)
        # ======================================================

        overview_parts = []

        if not tables_df.empty:
            overview_parts.append(build_overview(tables_df, "TABLE"))

        if not views_df.empty:
            overview_parts.append(build_overview(views_df, "VIEW"))

        if not procs_df.empty:
            overview_parts.append(build_overview(procs_df, "PROC"))

        overview_df = pd.concat(overview_parts, ignore_index=True)

        st.session_state["catalog_overview"] = overview_df


        tables_only = overview_df[overview_df["object_type"] == "TABLE"]
        views_only = overview_df[overview_df["object_type"] == "VIEW"]
        procs_only = overview_df[overview_df["object_type"] == "PROC"]


        cols_per_row = 3

        # ----------------------------
        # 📦 TABLES
        # ----------------------------

        if not tables_only.empty:

            st.markdown("## 📦 Tables")

            rows = [
                tables_only.iloc[i:i + cols_per_row]
                for i in range(0, len(tables_only), cols_per_row)
            ]

            for chunk in rows:

                cols = st.columns(cols_per_row)

                for i, row in chunk.iterrows():

                    with cols[list(chunk.index).index(i)]:

                        clicked = st.button(
                            f"📦 {row['full_name']}\n\n",
                            # f"Last Modified: {row.get('modify_date', 'N/A')}",
                            key=f"tbl_{row['full_name']}",
                            use_container_width=True,
                        )

                        if clicked:
                            st.session_state["catalog_selected"] = row["full_name"]
                            st.session_state["catalog_selected_type"] = "TABLE"
                            st.rerun()

        # ----------------------------
        # 👁️ VIEWS
        # ----------------------------

        if not views_only.empty:

            st.markdown("## 👁️ Views")

            rows = [
                views_only.iloc[i:i + cols_per_row]
                for i in range(0, len(views_only), cols_per_row)
            ]

            for chunk in rows:

                cols = st.columns(cols_per_row)

                for i, row in chunk.iterrows():

                    with cols[list(chunk.index).index(i)]:

                        clicked = st.button(
                            f"👁️ {row['full_name']}\n\n",
                            # f"Last Modified: {row.get('modify_date', 'N/A')}",
                            key=f"view_{row['full_name']}",
                            use_container_width=True,
                        )

                        if clicked:
                            st.session_state["catalog_selected"] = row["full_name"]
                            st.session_state["catalog_selected_type"] = "VIEW"
                            st.rerun()

        # ----------------------------
        # ⚙️ PROCS
        # ----------------------------

        if not procs_only.empty:

            st.markdown("## ⚙️ Procedures")

            rows = [
                procs_only.iloc[i:i + cols_per_row]
                for i in range(0, len(procs_only), cols_per_row)
            ]

            for chunk in rows:

                cols = st.columns(cols_per_row)

                for i, row in chunk.iterrows():

                    with cols[list(chunk.index).index(i)]:

                        clicked = st.button(
                            f"⚙️ {row['full_name']}",
                            key=f"proc_{row['full_name']}",
                            use_container_width=True,
                        )

                        if clicked:
                            st.session_state["catalog_selected"] = row["full_name"]
                            st.session_state["catalog_selected_type"] = "PROC"
                            st.rerun()
        
        # ======================================================
        # 📄 SELECTED OBJECT DETAILS PANEL
        # ======================================================

        # st.write("🧪 Selected:", st.session_state.get("catalog_selected"))

        if st.session_state.get("catalog_selected"):

            st.divider()

            selected = st.session_state["catalog_selected"]

            st.subheader(f"🗄 {selected}")

            dialect = st.session_state["dialect"]
            conn = st.session_state["conn"]

            parts = selected.split(".")

            if len(parts) == 3:
                db_name, schema_nm, table_name = parts
            elif len(parts) == 2:
                schema_nm, table_name = parts
                db_name = database
            else:
                table_name = parts[0]
                schema_nm = st.session_state.get("schema")
                db_name = database

            obj_type = st.session_state["catalog_selected_type"]

            st.write("🧪 Object type:", obj_type)

            meta_df = get_table_schema(
                conn,
                schema_nm,
                table_name,
                dialect,
                st.session_state.get("database"),
            )
            # st.dataframe(meta_df)
            # st.write("🧪 DEBUG column scan:")
            # st.write("Dialect:", dialect)
            # st.write("Database:", database)
            # st.write("Schema:", schema)
            # st.write("Table:", row["name"])


            # --------------------------------
            # 📘 FETCH VIEW DEFINITION
            # --------------------------------

            object_sql = None

            if obj_type in ["VIEW", "PROC"]:

                if dialect == "sqlserver":

                    sql = f"""
                    SELECT definition
                    FROM sys.sql_modules m
                    JOIN sys.objects o
                        ON m.object_id = o.object_id
                    JOIN sys.schemas s
                        ON o.schema_id = s.schema_id
                    WHERE s.name = '{schema}'
                    AND o.name = '{table_name}'
                    """

                else:  # snowflake

                    ddl_type = "VIEW" if obj_type == "VIEW" else "PROCEDURE"

                    sql = f"""
                    SELECT GET_DDL('{ddl_type}',
                        '{db_name}.{schema}.{table_name}')
                    """

                df_def = pd.read_sql(sql, conn)

                object_sql = df_def.iloc[0, 0]

            llm_doc = None
            lineage = {}
            downstream_views = None
            downstream_procs = None


            if obj_type in ["VIEW", "PROC"] and object_sql:

                with st.spinner("🧠 Generating technical summary with GenAI..."):

                    llm_doc = generate_sql_documentation(
                        object_name=selected,
                        object_type=obj_type.lower(),
                        object_sql=object_sql
                    )

                    log_app_action(
                        f"GENERATE_{obj_type}_SUMMARY",
                        object_name=selected,
                        object_type=obj_type,
                    )

                st.markdown("## 🧠 Description")
                st.markdown(llm_doc or "No description generated.")

                lineage = extract_sql_lineage(object_sql)

                log_app_action(
                    "EXTRACT_LINEAGE",
                    object_name=selected,
                    object_type=obj_type,
                    details=lineage,
                )

                st.markdown("## 🌐 Lineage")

                st.markdown("### Tables Used")
                st.write(lineage.get("tables", []))

                st.markdown("### Views Used")
                st.write(lineage.get("views", []))

                st.markdown("### Procedures Used")
                st.write(lineage.get("procedures", []))

                st.markdown("### Functions Used")
                st.write(lineage.get("functions", []))

                downstream_views = None
                downstream_procs = None

                st.markdown("## 🔁 Relations")

                st.markdown("### Called by Views")

                downstream_views = find_downstream_views(
                    conn, db_name, schema, table_name
                )

                if downstream_views is not None and not downstream_views.empty:
                    st.dataframe(downstream_views)
                else:
                    st.write("None")

                st.markdown("### Called by Procedures")

                if dialect == "sqlserver":

                    downstream_procs = find_downstream_procs(
                        conn, db_name, schema, table_name
                    )

                    if downstream_procs is not None and not downstream_procs.empty:
                        st.dataframe(downstream_procs)
                    else:
                        st.write("None")

                # ======================================================
                # 🕸 LINEAGE GRAPH (CATALOG VIEW)
                # ======================================================

                # ======================================================
                # 🕸 LINEAGE GRAPH (CATALOG VIEW)
                # ======================================================

                st.markdown("## 🕸 Lineage Graph")

                edges = []

                base_object = selected.upper()

                # --------------------------
                # UPSTREAM → BASE
                # --------------------------

                for t in lineage.get("tables", []):
                    edges.append((t.upper(), base_object))

                for v in lineage.get("views", []):
                    edges.append((v.upper(), base_object))

                for p in lineage.get("procedures", []):
                    edges.append((p.upper(), base_object))

                # --------------------------
                # BASE → DOWNSTREAM
                # --------------------------

                for v in _build_full_names(downstream_views):
                    edges.append((base_object, v.upper()))

                for p in _build_full_names(downstream_procs):
                    edges.append((base_object, p.upper()))

                if not edges:
                    st.info("No lineage relationships found.")
                else:

                    import networkx as nx
                    from pyvis.network import Network
                    import tempfile

                    G = nx.DiGraph()

                    # Add nodes & edges
                    for src, tgt in edges:
                        G.add_node(src)
                        G.add_node(tgt)
                        G.add_edge(src, tgt)

                    graph_path = render_graph(G)

                    with open(graph_path, "r", encoding="utf-8") as f:
                        st.components.v1.html(f.read(), height=650)


                overview_df = st.session_state.get("catalog_overview")

                row_meta = overview_df[
                    overview_df["full_name"] == selected
                ].iloc[0]

                modify_date = row_meta.get("modify_date")
                create_date = row_meta.get("create_date")


                html_report = build_view_html_report(
                    view_name=selected,
                    modify_date=modify_date,
                    create_date=create_date,
                    sql_text=object_sql,
                    llm_doc=llm_doc,
                    lineage=lineage,
                    downstream_views=downstream_views,
                    downstream_procs=downstream_procs,
                )

                st.download_button(
                    label=f"📥 Download {obj_type} Report (HTML)",
                    data=html_report,
                    file_name=f"{selected.replace('.', '_')}_{obj_type.lower()}_catalog.html",
                    mime="text/html",
                )
                if "meta_df" not in locals() or meta_df is None:
                    meta_df = pd.DataFrame(
                        columns=["column_name", "data_type", "length"]
                    )



            meta_df.columns = [c.lower() for c in meta_df.columns]

            rename_map = {
                "column_name": "column_name",
                "data_type": "data_type",
                "character_maximum_length": "length",
                "is_nullable": "nullable"
            }

            meta_df = meta_df.rename(columns=rename_map)

            # Length cleanup
            if "length" not in meta_df:
                meta_df["length"] = None

            # Constraint flag
            meta_df["constraint"] = meta_df.apply(
                lambda r: "YES"
                if any(
                    x in str(r).upper()
                    for x in ["PRIMARY", "FOREIGN", "UNIQUE"]
                )
                else "NO",
                axis=1
            )

            # Editable columns
            meta_df["pii"] = False
            meta_df["tag"] = ""

            # Limit tag length to 20
            meta_df["tag"] = meta_df["tag"].astype(str).str[:20]

            display_cols = [
                "column_name",
                "data_type",
                "length",
                "constraint",
                "pii",
                "tag"
            ]
            # Always show PROC metadata (read-only in DB Change)
            if obj_type == "PROC":
                st.markdown("## ⚙️ Procedure Parameters")
                st.dataframe(
                    meta_df[display_cols[:3]],
                    use_container_width=True
                )

            obj_type_norm = str(st.session_state["catalog_selected_type"]).upper().strip()

            if obj_type_norm == "TABLE":

                editable_df = st.data_editor(
                    meta_df[display_cols],
                    num_rows="fixed",
                    use_container_width=True,
                    column_config={
                        "pii": st.column_config.CheckboxColumn("PII"),
                        "tag": st.column_config.TextColumn("Tag", max_chars=20),
                    },
                )
                if st.button("💾 Save Catalog Metadata"):

                    save_catalog_to_sqlite(
                        editable_df,
                        source_object=selected,
                        object_type=st.session_state["catalog_selected_type"]
                    )

                    st.success("✅ Metadata saved to SQLite catalog")
                    log_app_action(
                        "SAVE_CATALOG_METADATA",
                        object_name=selected,
                        object_type=st.session_state["catalog_selected_type"],
                    )

            else:
                st.info("ℹ️ Views & Procedures are read-only in DB Catalog mode.")


                # ----------------------------
                # 💾 SAVE TO SQLITE
                # ----------------------------

                




        # with st.expander("Browse Objects"):

        #     for t, objs in snapshot["objects"].items():
        #         st.markdown(f"### {t}")
        #         for k in sorted(objs.keys()):
        #             st.text(k)

    # ======================================================
    # 🚨 CHANGE DETECTION
    # ======================================================

    with tab2:

        st.subheader("🚨 Change Detection")

        # compare_prev = st.toggle(
        #     "Compare against previous snapshot",
        #     value=False,
        # )

        if st.button("🔄 Refresh & Detect Changes"):

            with st.spinner("🔄 Re-scanning database..."):

                tables_df = get_tables(conn, dialect, schema, database)
                views_df = get_views(conn, dialect, schema, database)
                procs_df = get_procedures(conn, dialect, schema)
                # -------------------------------
                # 🔁 NORMALIZE METADATA (REFRESH)
                # -------------------------------

                tables_df.columns = [c.lower() for c in tables_df.columns]
                views_df.columns = [c.lower() for c in views_df.columns]
                procs_df.columns = [c.lower() for c in procs_df.columns]

                tables_df = tables_df.rename(
                    columns={
                        "table_schema": "schema",
                        "table_name": "name",
                    }
                )

                views_df = views_df.rename(
                    columns={
                        "table_schema": "schema",
                        "table_name": "name",
                    }
                )

                procs_df = procs_df.rename(
                    columns={
                        "routine_schema": "schema",
                        "routine_name": "name",
                    }
                )

                tables_df["full_name"] = tables_df["schema"] + "." + tables_df["name"]
                views_df["full_name"] = views_df["schema"] + "." + views_df["name"]
                procs_df["full_name"] = procs_df["schema"] + "." + procs_df["name"]


                all_columns = []

                for _, row in tables_df.iterrows():

                    df_cols = get_table_schema(
                        conn,
                        schema,
                        row["name"],
                        dialect,
                        database,
                    )
                    # -------------------------------
                    # 🔁 NORMALIZE COLUMN METADATA
                    # -------------------------------

                    df_cols.columns = [c.lower() for c in df_cols.columns]

                    df_cols = df_cols.rename(
                        columns={
                            "column_name": "column_name",   # keep
                            "column": "column_name",
                            "name": "column_name",
                            "data_type": "data_type",
                            "type": "data_type",
                            "is_nullable": "nullable",
                            "nullable": "nullable",
                            "character_maximum_length": "length",
                        }
                    )


                    df_cols["schema"] = schema
                    df_cols["object_name"] = row["name"]
                    df_cols["object_type"] = "TABLE"
                    df_cols["full_name"] = (
    schema + "." + row["name"]
)

                    all_columns.append(df_cols)

                import pandas as pd

                all_columns_df = (
                    pd.concat(all_columns, ignore_index=True)
                    if all_columns
                    else pd.DataFrame()
                )

                if dialect == "sqlserver":
                    timestamps_df = get_object_timestamps_sqlserver(
                        conn,
                        schema,
                    )
                else:
                    timestamps_df = get_object_timestamps_snowflake(
                        conn,
                        database,
                        schema,
                    )
                # -------------------------------
                # 🔁 NORMALIZE TIMESTAMPS METADATA (SAFE)
                # -------------------------------

                timestamps_df.columns = [c.lower() for c in timestamps_df.columns]

                # Map possible name columns
                if "name" not in timestamps_df.columns:
                    for cand in ["object_name", "table_name", "routine_name"]:
                        if cand in timestamps_df.columns:
                            timestamps_df = timestamps_df.rename(columns={cand: "name"})
                            break

                # Map object type
                if "object_type" not in timestamps_df.columns:
                    if "type_desc" in timestamps_df.columns:
                        timestamps_df = timestamps_df.rename(columns={"type_desc": "object_type"})

                # -------------------------------
                # 🧱 BUILD FULL NAME (only if possible)
                # -------------------------------

                if "schema" in timestamps_df.columns and "name" in timestamps_df.columns:

                    timestamps_df["schema"] = timestamps_df["schema"].astype(str)
                    timestamps_df["name"] = timestamps_df["name"].astype(str)

                    timestamps_df["full_name"] = (
                        timestamps_df["schema"] + "." + timestamps_df["name"]
                    )

                else:
                    st.error(
                        f"❌ timestamps_df missing required cols: {timestamps_df.columns.tolist()}"
                    )
                    st.stop()

                # -------------------------------
                # 🔁 NORMALIZE OBJECT TYPE VALUES
                # -------------------------------

                if "object_type" in timestamps_df.columns:

                    timestamps_df["object_type"] = (
                        timestamps_df["object_type"]
                        .astype(str)
                        .str.upper()
                    )


                # st.write("🧪 timestamps_df columns:", timestamps_df.columns.tolist())
                # st.write(timestamps_df.head())


                # -------------------------------
                # 🧱 BUILD FULL NAME + TYPE
                # -------------------------------

                timestamps_df["schema"] = timestamps_df["schema"].astype(str)
                timestamps_df["name"] = timestamps_df["name"].astype(str)

                timestamps_df["full_name"] = (
                    timestamps_df["schema"] + "." + timestamps_df["name"]
                )

                timestamps_df["object_type"] = (
                    timestamps_df["object_type"]
                    .astype(str)
                    .str.upper()
                )
                # -------------------------------
                # 🎯 MAP ENGINE TYPES → CANONICAL
                # -------------------------------

                TYPE_MAP = {
                    "USER_TABLE": "TABLE",
                    "BASE TABLE": "TABLE",
                    "TABLE": "TABLE",

                    "VIEW": "VIEW",

                    "SQL_STORED_PROCEDURE": "PROC",
                    "STORED_PROCEDURE": "PROC",
                    "PROCEDURE": "PROC",
                    "PROC": "PROC",
                }

                timestamps_df["object_type"] = (
                    timestamps_df["object_type"]
                    .map(TYPE_MAP)
                    .fillna(timestamps_df["object_type"])
                )

                current_snapshot = build_catalog_snapshot(
                    database=database,
                    schema=schema,
                    dialect=dialect,
                    tables_df=tables_df,
                    views_df=views_df,
                    procs_df=procs_df,
                    columns_df=all_columns_df,
                    timestamps_df=timestamps_df,
                )

                current_id = persist_snapshot_sqlite(current_snapshot)

            # --------------------------------------------
            # BASE SELECTION
            # --------------------------------------------

            # if compare_prev:
            #     base_snapshot = get_baseline_snapshot_sqlite()
            #     st.info("Comparing PREVIOUS → CURRENT")
            # else:
            #     base_snapshot = get_baseline_snapshot_sqlite()
            #     st.info("Comparing BASELINE → CURRENT")

            # --------------------------------------------
            # DIFF
            # --------------------------------------------

            # --------------------------------------------
            # ✅ OPTION-B ONLY — PREVIOUS → CURRENT
            # --------------------------------------------

            prev_snapshot = st.session_state.get("previous_snapshot")

            if not prev_snapshot:

                st.info("📌 First scan — no previous snapshot yet. Next refresh will detect changes.")

                st.session_state["previous_snapshot"] = current_snapshot
                st.stop()

            st.info("🔄 Incremental detection: PREVIOUS → CURRENT")

            changes = compare_snapshots(
                prev_snapshot,
                current_snapshot,
            )

            # Save for next run
            st.session_state["previous_snapshot"] = current_snapshot
            st.json(changes)
            
            # --------------------------------------------
            # RISK + LLM
            # --------------------------------------------

            st.subheader("🧠 Risk Assessment")

            analysis_prompt = build_change_analysis_prompt(changes)

            llm_response = analyze_schema_changes_llm(
                prompt=analysis_prompt
)

            st.markdown(llm_response)

            st.subheader("🚦 Severity Matrix")


            import re

            levels = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

            for lvl in levels:
                if re.search(lvl, llm_response.upper()):
                    levels[lvl] += 1

            risk_df = pd.DataFrame(
                [
                    {"Severity": k, "Detected": v}
                    for k, v in levels.items()
                ]
            )

            st.dataframe(risk_df, use_container_width=True)

            # --------------------------------------------
            # IMPACT GRAPH
            # --------------------------------------------
            # ============================================================
            # 🕸 IMPACT LINEAGE — DRIVEN BY LLM SEVERITY
            # ============================================================

            st.markdown(
                """
            ### 🎨 Risk Legend
            🟥 **High Impact**  
            🟨 **Medium Impact**  
            🟩 **Low Impact**  
            🟪 **Procedure**  
            🟦 **Base Table**
            """
            )

            # ------------------------------------------------------------
            # 🔎 Extract base object from changes
            # ------------------------------------------------------------

            changed_objects = set()

            for _, items in changes.items():
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and item.get("object"):
                            changed_objects.add(item["object"].upper())

            if not changed_objects:
                st.warning("No changed objects detected for lineage.")
                st.stop()

            base_object = next(iter(changed_objects))

            # ------------------------------------------------------------
            # 📡 SQL SERVER LINEAGE LOOKUP
            # ------------------------------------------------------------

            lineage_rows = []

            for obj in changed_objects:

                schema_nm, name = obj.split(".", 1)

                impacted = get_impacted_objects_sqlserver(
                    conn,
                    schema_nm,
                    name,
                )

                if not impacted.empty:
                    lineage_rows.extend(impacted.to_dict("records"))

            lineage_df = pd.DataFrame(lineage_rows)

            if lineage_df.empty:
                st.warning("No downstream dependencies found in DB.")
                st.stop()

            # ------------------------------------------------------------
            # 🧱 Normalize for graph engine
            # ------------------------------------------------------------

            lineage_df["base_full"] = (
                lineage_df["base_schema"] + "." +
                lineage_df["base_object"]
            ).str.upper()

            lineage_df["dependent_full"] = (
                lineage_df["dependent_schema"] + "." +
                lineage_df["dependent_object"]
            ).str.upper()

            graph_df = lineage_df.copy()
            graph_df["view_name"] = graph_df["dependent_full"]

            # ------------------------------------------------------------
            # 🎯 PARSE SEVERITY FROM LLM
            # ------------------------------------------------------------

            # ------------------------------------------------------------
            # 🎯 ROBUST SEVERITY EXTRACTION FROM LLM
            # ------------------------------------------------------------

            import re

            llm_text = (
                llm_response
                .replace("\u200b", "")
                .replace("\xa0", " ")
            )

            patterns = [
                r"severity\s*:\s*(high|medium|low)",
                r"risk\s*assessment.*?(high|medium|low)",
                r"safe\s*or\s*breaking.*?(breaking|safe)",
            ]

            global_sev = None

            for p in patterns:
                m = re.search(p, llm_text, flags=re.IGNORECASE | re.DOTALL)
                if m:
                    val = m.group(1).upper()
                    if val == "BREAKING":
                        global_sev = "HIGH"
                    elif val == "SAFE":
                        global_sev = "LOW"
                    else:
                        global_sev = val
                    break

            # Hard safety fallback
            if not global_sev:
                if "drop column" in llm_text.lower() or "removed column" in llm_text.lower():
                    global_sev = "HIGH"
                else:
                    global_sev = "LOW"

            st.info(f"📌 LLM-Derived Severity: {global_sev}")


            # ------------------------------------------------------------
            # 🚦 Build risk map from LLM
            # ------------------------------------------------------------

            risk_rows = []

            # Base object
            risk_rows.append(
                {
                    "object": base_object,
                    "severity": global_sev,
                }
            )

            # All dependents inherit risk
            for dep in graph_df["view_name"].unique():

                risk_rows.append(
                    {
                        "object": dep.upper(),
                        "severity": global_sev,
                    }
                )

            risk_obj_df = pd.DataFrame(risk_rows)
            graph_df["view_name"] = graph_df["view_name"].str.upper()
            risk_obj_df["object"] = risk_obj_df["object"].str.upper()
            base_object = base_object.upper()


            # ------------------------------------------------------------
            # 🌐 Build graph
            # ------------------------------------------------------------

            G = build_dependency_graph(
    base_object=base_object.upper(),
    impacted_views_df=graph_df,
    risk_df=risk_obj_df,
)


            graph_path = render_graph(G)

            with open(graph_path, "r", encoding="utf-8") as f:
                html(f.read(), height=650)





