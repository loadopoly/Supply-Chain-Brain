"""Page 6 — Connectors: manage DBs and external apps (req 4 + 5)."""
from pathlib import Path
import sys
import streamlit as st
from src.brain.dynamic_insight import render_dynamic_brain_insight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.brain import load_config
from src.brain.db_registry import bootstrap_default_connectors, list_connectors, get
from src.brain import ips_freight

# set_page_config handled by app.py st.navigation()
st.session_state["_page"] = "connectors"
bootstrap_default_connectors()

st.markdown("## 🔌 Connectors")
st.caption("Pluggable databases + external applications · Add by editing `config/brain.yaml`")

ctx = {k: v for k, v in st.session_state.items() if not str(k).startswith('_') and not callable(v)}
render_dynamic_brain_insight('Connectors', ctx)
st.divider()


st.subheader("Registered connectors")

# Status summary row
_conns = list_connectors()
if _conns:
    _status_cols = st.columns(len(_conns))
    for _i, _c in enumerate(_conns):
        if _c._handle is not None:
            _status_cols[_i].success(f"🟢 **{_c.name}** · `{_c.kind}`")
        else:
            _status_cols[_i].warning(f"🟡 **{_c.name}** · `{_c.kind}`")
    st.write("")

for c in list_connectors():
    with st.expander(f"{c.name} · {c.kind}", expanded=False):
        st.write(c.description or "—")
        if st.button(f"Reset {c.name} handle", key=f"reset_{c.name}"):
            c.reset()
            st.success(f"{c.name} handle reset.")
        if st.button(f"Probe {c.name}", key=f"probe_{c.name}"):
            try:
                h = c.handle()
                st.success(f"Handle ready: {type(h).__name__}")
            except Exception as exc:
                st.error(str(exc))

st.markdown("---")
st.subheader("External applications")
cfg = load_config().get("external_apps", {})
for name, conf in cfg.items():
    with st.expander(f"🌐 {name}", expanded=True):
        st.json(conf)
        if name == "ips_freight":
            if st.button("Health check IPS Freight", key="ips_health"):
                st.json(ips_freight.health())
            link = conf.get("dashboard")
            if link:
                st.link_button("Open IPS Freight dashboard ↗", link)

st.markdown("---")
st.subheader("➕ Add a new database")
st.code("""
# 1) edit pipeline/config/brain.yaml — add it under `tables:` and `external_apps:`
# 2) create pipeline/src/connections/<your_db>.py exposing get_connection()
# 3) call db_registry.register(Connector(...)) in db_registry.bootstrap_default_connectors()
""", language="text")
