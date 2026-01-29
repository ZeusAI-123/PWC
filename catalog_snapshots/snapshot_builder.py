from datetime import datetime
import uuid


def build_catalog_snapshot(
    database,
    schema,
    dialect,
    tables_df,
    views_df,
    procs_df,
    columns_df,
    timestamps_df=None,
):

    snapshot = {
        "metadata": {
            "database": database,
            "schema": schema,
            "dialect": dialect,
            "timestamp": datetime.utcnow().isoformat(),
            "hash": None,
        },
        "objects": {
            "TABLE": {},
            "VIEW": {},
            "PROC": {},
        },
    }

    # ---------------------------
    # ⬛ Columns
    # ---------------------------

    if columns_df is not None:

        for _, row in columns_df.iterrows():

            obj = row["full_name"]
            col = row["column_name"]

            if "." not in obj:
                continue

            obj_type = row["object_type"]

            snapshot["objects"][obj_type].setdefault(
                obj,
                {
                    "columns": {},
                    "keys": [],
                    "indexes": [],
                },
            )

            snapshot["objects"][obj_type][obj]["columns"][col] = {
                "type": row.get("data_type"),
                "nullable": row.get("nullable"),
                "length": row.get("length"),
            }

# ---------------------------
# ⬛ Keys / Indexes
# ---------------------------

    if "primary_key" in columns_df.columns:

        for obj, grp in columns_df.groupby("full_name"):

            obj_type = grp["object_type"].iloc[0]

            snapshot["objects"][obj_type].setdefault(
                obj,
                {"columns": {}, "keys": [], "indexes": []}
            )

            snapshot["objects"][obj_type][obj]["keys"] = (
                grp[grp["primary_key"] == True]["column_name"].tolist()
            )

    # ---------------------------
    # ⬛ Timestamps
    # ---------------------------

    if timestamps_df is not None:

        for _, r in timestamps_df.iterrows():

            obj_type = r["object_type"]
            full = r["full_name"]

            snapshot["objects"][obj_type].setdefault(full, {})

            snapshot["objects"][obj_type][full]["created"] = str(
                r.get("create_date")
            )

            snapshot["objects"][obj_type][full]["modified"] = str(
                r.get("modify_date")
            )
    

    snapshot["snapshot_id"] = str(uuid.uuid4())


    return snapshot
