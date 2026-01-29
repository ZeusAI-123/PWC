import networkx as nx
from pyvis.network import Network
import tempfile


def build_dependency_graph(
    base_object,
    impacted_views_df=None,
    impacted_procs_df=None,
    risk_df=None,
):
    """
    Creates interactive dependency graph.
    Handles flexible column naming.
    """

    G = nx.DiGraph()

    # Root node
    G.add_node(base_object, label=base_object, color="#4F46E5")

    # ------------------------
    # Risk lookup
    # ------------------------
    risk_map = {}

    if risk_df is not None and not risk_df.empty:
        for _, r in risk_df.iterrows():
            name = r.get("view_name") or r.get("object")
            if name:
                risk_map[name.upper()] = r.get("risk") or r.get("severity")

    # ------------------------
    # Views
    # ------------------------
    if impacted_views_df is not None:

        for _, row in impacted_views_df.iterrows():

            # try best column
            view = (
                row.get("view_name")
                or row.get("full_name")
                or row.get("name")
            )

            if not view:
                continue

            view = str(view).upper()

            color = "#CBD5E1"  # default gray

            risk = risk_map.get(view)

            if risk:
                risk = risk.upper()
                if risk == "HIGH":
                    color = "#EF4444"
                elif risk == "MEDIUM":
                    color = "#F59E0B"
                elif risk == "LOW":
                    color = "#22C55E"

            G.add_node(view, label=view, color=color)
            G.add_edge(base_object, view)

    # ------------------------
    # Procedures (optional)
    # ------------------------
    if impacted_procs_df is not None:

        for _, row in impacted_procs_df.iterrows():

            proc = (
                row.get("routine_name")
                or row.get("procedure_name")
                or row.get("name")
            )

            if not proc:
                continue

            proc = str(proc).upper()

            G.add_node(proc, label=proc, color="#A855F7")
            G.add_edge(base_object, proc)

    return G


def render_graph(G):

    net = Network(
        height="600px",
        width="100%",
        directed=True,
        bgcolor="#ffffff",
        font_color="#1f2933",
    )

    # -----------------------------
    # Disable chaotic physics
    # -----------------------------
    net.toggle_physics(False)

    # -----------------------------
    # Add nodes with bigger fonts
    # -----------------------------
    for node, data in G.nodes(data=True):

        net.add_node(
            node,
            label=data.get("label", node),
            color=data.get("color", "#CBD5E1"),
            shape="box",
            font={"size": 20},
        )

    # -----------------------------
    # Add edges
    # -----------------------------
    for src, tgt in G.edges():

        net.add_edge(
            src,
            tgt,
            arrows="to",
            color="#64748B",
            width=2,
        )

    # -----------------------------
    # Hierarchical layout
    # -----------------------------
    net.set_options(
        """
        {
          "layout": {
            "hierarchical": {
              "enabled": true,
              "direction": "LR",
              "sortMethod": "directed",
              "levelSeparation": 200,
              "nodeSpacing": 200
            }
          },
          "physics": {
            "enabled": false
          }
        }
        """
    )

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(tmp.name)

    return tmp.name
