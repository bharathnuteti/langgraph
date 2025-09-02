import streamlit as st
import json
from streamlit_mermaid import st_mermaid
from test import Engine, InMemoryStore  # your LangGraph 0.6.6 backend
from json_store import JSONStore   # persistent store from earlier step
from typing import Optional
import json
from typing import Optional, Dict, Any, List
import json
from typing import Dict, Any, List
import streamlit as st
from streamlit_mermaid import st_mermaid
from test import Engine, InMemoryStore, InstanceMeta

# Mermaid diagram generator
def workflow_mermaid(meta: Dict[str, Any]) -> str:
    node_ids = {
        "Validate Request": "A",
        "Gather Claim Info": "B",
        "Identify Accounts & Process Decision": "D",
        "Cancel CWD Request": "C",
        "Hold Request": "E",
        "Apply Temporary Suppression": "F",
        "Fulfill Case and Detect": "G",
        "END": "H",
    }

    visited_styles = []
    for step in meta.get("steps_history", []):
        node = step.get("node")
        if node in node_ids:
            visited_styles.append(f"style {node_ids[node]} fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;")

    current_node = meta.get("last_node") or (
        meta["steps_history"][-1]["node"] if meta.get("steps_history") else "Validate Request"
    )
    if current_node in node_ids:
        visited_styles.append(f"style {node_ids[current_node]} fill:#ffecb3,stroke:#ff6f00,stroke-width:4px;")

    styles = "\n    ".join(visited_styles)
    return f"""
graph TD
    A[Validate Request] -->|yes| B[Gather Claim Info]
    A -->|no| C[Cancel CWD Request]
    B --> D[Identify Accounts & Process Decision]
    D -->|cancel| C
    D -->|hold| E[Hold Request]
    D -->|suppress| F[Apply Temporary Suppression]
    E -->|resume| F
    E -->|abort| C
    F -->|yes| G[Fulfill Case and Detect]
    F -->|no| C
    G --> H[END]
    C --> H
    {styles}
"""

# Init engine
if "engine" not in st.session_state:
    st.session_state.store = InMemoryStore()
    st.session_state.engine = Engine(st.session_state.store)

engine: Engine = st.session_state.engine

st.set_page_config(page_title="Claim Workflow Manager", layout="wide")
st.title("📋 Claim Workflow Manager")

# Start workflow
st.header("🚀 Start a New Workflow")
with st.form("start_form"):
    customer_id = st.text_input("Customer ID")
    started_by = st.text_input("Started by (user)")
    start_btn = st.form_submit_button("Start Workflow")
    if start_btn:
        if not customer_id or not started_by:
            st.error("Please provide both Customer ID and Started by.")
        else:
            inst_id, out = engine.start(customer_id, started_by)
            st.success(f"Workflow {inst_id} started")
            st.json(out)

# Resume workflow
st.header("⏩ Resume a Workflow")
with st.form("resume_form"):
    resume_id = st.text_input("Instance ID to resume")
    actor = st.text_input("Actor resuming")
    updates_str = st.text_area("Updates (JSON)", '{"validate": "yes"}')
    resume_btn = st.form_submit_button("Resume Workflow")
    if resume_btn:
        try:
            updates = json.loads(updates_str or "{}")
            out = engine.resume(resume_id, actor, updates)
            st.success(f"Workflow {resume_id} resumed by {actor}")
            st.json(out)
        except json.JSONDecodeError:
            st.error("Invalid JSON in updates field.")

# All workflows
st.header("📋 All Workflows")
instances: List[InstanceMeta] = engine.list_instances()
if instances:
    for m in instances:
        meta = m.to_dict()
        if not meta.get("last_node"):
            meta["last_node"] = "Validate Request"
        with st.expander(f"Workflow {meta['instance_id']} — {meta['status']}"):
            st.write("**Customer ID:**", meta["customer_id"])
            st.write("**Workflow Name:**", meta["workflow_name"])
            st.write("**Started By:**", meta["started_by"])
            st.write("**Last Actor:**", meta["last_actor"])
            st.write("**Last Node:**", meta["last_node"])
            st.write("**Start Time:**", meta["start_time"])
            st.write("**End Time:**", meta["end_time"])
            st.write("**Status:**", meta["status"])

            st.markdown("**Steps History:**")
            if meta["steps_history"]:
                for i, step in enumerate(meta["steps_history"], start=1):
                    st.markdown(f"**Step {i}:**")
                    st.json(step)
            else:
                st.write("_No steps recorded yet_")

            st.markdown("**Workflow Diagram:**")
            st_mermaid(workflow_mermaid(meta))
else:
    st.info("No workflows yet.")
