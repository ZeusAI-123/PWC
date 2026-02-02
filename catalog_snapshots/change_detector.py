import json
import datetime
import pandas as pd

# --------------------------------------------------
# 🔍 Snapshot Comparison Engine
# --------------------------------------------------

def compare_snapshots(baseline: dict, current: dict) -> dict:
    """
    Compare two catalog snapshots.
    Returns structured change log.
    """

    changes = {
        "metadata": {
            "baseline_timestamp": baseline["metadata"]["timestamp"],
            "current_timestamp": current["metadata"]["timestamp"],
            "baseline_hash": baseline["metadata"]["hash"],
            "current_hash": current["metadata"]["hash"],
        },
        "objects_added": [],
        "objects_removed": [],
        "column_added": [],
        "column_removed": [],
        "column_renamed": [],
        "datatype_changed": [],
        "nullability_changed": [],
        "length_changed": [],
         # NEW
    "primary_key_changed": [],
    "foreign_key_changed": [],
    "indexes_changed": [],
    }

    for obj_type in ["TABLE", "VIEW", "PROC"]:

        base_objs = baseline["objects"].get(obj_type, {})
        curr_objs = current["objects"].get(obj_type, {})

        base_set = set(base_objs.keys())
        curr_set = set(curr_objs.keys())

        # ---------------------------
        # 🟢 Added / 🔴 Removed
        # ---------------------------

        for o in sorted(curr_set - base_set):
            changes["objects_added"].append(
                {"object": o, "type": obj_type}
            )

        for o in sorted(base_set - curr_set):
            changes["objects_removed"].append(
                {"object": o, "type": obj_type}
            )

        # ---------------------------
        # 🔵 Existing objects
        # ---------------------------

        for obj in base_set & curr_set:

            base_cols = base_objs[obj].get("columns", {})
            curr_cols = curr_objs[obj].get("columns", {})

            bset = set(base_cols)
            cset = set(curr_cols)

            # ➕ Column added
            for c in cset - bset:
                changes["column_added"].append(
                    {
                        "object": obj,
                        "type": obj_type,
                        "column": c,
                        "after": curr_cols[c],
                    }
                )

            # ➖ Column removed
            for c in bset - cset:
                changes["column_removed"].append(
                    {
                        "object": obj,
                        "type": obj_type,
                        "column": c,
                        "before": base_cols[c],
                    }
                )

            # ✏️ Column renamed (simple heuristic)
            if len(bset - cset) == 1 and len(cset - bset) == 1:

                old = list(bset - cset)[0]
                new = list(cset - bset)[0]

                if base_cols[old]["type"] == curr_cols[new]["type"]:
                    changes["column_renamed"].append(
                        {
                            "object": obj,
                            "type": obj_type,
                            "before": old,
                            "after": new,
                        }
                    )

            # 🔁 Column modifications
            for c in bset & cset:

                b = base_cols[c]
                n = curr_cols[c]

                if b.get("type") != n.get("type"):
                    changes["datatype_changed"].append(
                        {
                            "object": obj,
                            "type": obj_type,
                            "column": c,
                            "before": b.get("type"),
                            "after": n.get("type"),
                        }
                    )

                if b.get("nullable") != n.get("nullable"):
                    changes["nullability_changed"].append(
                        {
                            "object": obj,
                            "type": obj_type,
                            "column": c,
                            "before": b.get("nullable"),
                            "after": n.get("nullable"),
                        }
                    )

                b_len = b.get("length")
                n_len = n.get("length")

                # Ignore NULL -> NULL
                if not (pd.isna(b_len) and pd.isna(n_len)):
                    if b_len != n_len:
                        changes["length_changed"].append(
                            {
                                "object": obj,
                                "type": obj_type,
                                "column": c,
                                "before": b_len,
                                "after": n_len,
                            }
                        )
            # ---------------------------
            # 🔑 Key / Index Changes
            # ---------------------------

            base_keys = base_objs[obj].get("keys", [])
            curr_keys = curr_objs[obj].get("keys", [])

            if set(base_keys) != set(curr_keys):
                changes["primary_key_changed"].append(
                    {
                        "object": obj,
                        "type": obj_type,
                        "before": base_keys,
                        "after": curr_keys,
                    }
                )

            base_idx = base_objs[obj].get("indexes", [])
            curr_idx = curr_objs[obj].get("indexes", [])

            if set(base_idx) != set(curr_idx):
                changes["indexes_changed"].append(
                    {
                        "object": obj,
                        "type": obj_type,
                        "before": base_idx,
                        "after": curr_idx,
                    }
                )


    return changes


# --------------------------------------------------
# 💾 Persist change log
# --------------------------------------------------

def save_change_log(changes: dict) -> str:
    """
    Save change log to disk.
    """

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    path = f"catalog_snapshots/change_log_{ts}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(changes, f, indent=2)

    return path
