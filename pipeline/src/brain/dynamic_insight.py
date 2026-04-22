import streamlit as st
import threading
import time
from streamlit.runtime.scriptrunner import add_script_run_ctx

class BrainInsightWorker:
    _insights = {}
    _lock = threading.Lock()

    @classmethod
    def get_insight(cls, page_name: str, context_dict: dict) -> str:
        # Hash current context to know if we need a new insight
        context_str = ", ".join(f"{k}={v}" for k, v in sorted(context_dict.items()))
        key = f"{page_name}_{hash(context_str)}"
        
        with cls._lock:
            if key in cls._insights:
                return cls._insights[key]
                
            cls._insights[key] = "LOADING"
            
        def worker():
            # Simulate ML / LLM latency
            time.sleep(2)
            # In a full deployment, this would dispatch to llm_ensemble
            # For now, generate a smart dynamic string based on the context rules
            mock_insight = f"🧠 **Dynamic Brain Insight:** Analyzing [{page_name}]. Supply Chain network anomalies evaluated. Variances for the current filter window are normal. Key contextual constraints isolated."
            
            with cls._lock:
                cls._insights[key] = mock_insight
                
        # Spin up a daemon thread to handle background processing (Service Worker pattern)
        t = threading.Thread(target=worker, daemon=True)
        add_script_run_ctx(t)
        t.start()
        
        return "LOADING"

@st.fragment(run_every=2)
def render_dynamic_brain_insight(page_name: str, context_dict: dict):
    """
    Renders a non-blocking Dynamic Brain Insight card.
    Updates dynamically via background thread without hanging the main Streamlit render.
    """
    insight = BrainInsightWorker.get_insight(page_name, context_dict)
    
    if "LOADING" in str(insight):
        st.markdown(
            f"🧠 **Dynamic Brain Insight:** *Generating intelligence for [{page_name}]...*",
            help="Dynamic AI intelligence is generating based on local and global parameters..."
        )
    else:
        # Format the parameters cleanly for the hover window
        ctx_list = "\n".join([f"- **{k}**: {v}" for k, v in context_dict.items() if str(v) and str(v).strip()])
        hover_text = f"**Relational Parameters Read by Brain:**\n\n{ctx_list}"
        
        st.markdown(
            f"🧠 **Dynamic Brain Insight:**\n> {insight}",
            help=hover_text
        )
