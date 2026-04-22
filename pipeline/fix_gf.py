with open('src/brain/global_filters.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''        st.session_state["g_date_start"] = sd
        st.session_state["g_date_end"] = ed'''

new_str = '''        if st.session_state.get("g_date_start") != sd or st.session_state.get("g_date_end") != ed:
            st.session_state["g_date_start"] = sd
            st.session_state["g_date_end"] = ed
            for k in list(st.session_state.keys()):
                if k.endswith('_sql') or k == 'otd_where' or k == 'bw_sql' or k == 'eoq_sql':
                    del st.session_state[k]
            st.cache_data.clear()'''

text = text.replace(replacement, new_str)

with open('src/brain/global_filters.py', 'w', encoding='utf-8') as f:
    f.write(text)
