import sys
path = "pipeline/pages/3_OTD_Recursive.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

idx = content.find("with tab_own:")
if idx != -1:
    new_tail = """with tab_daily:
    st.subheader("Daily Review Worklists")
    if not daily_worklists:
        st.info("No worklist sheets (e.g. 'Missed Yesterday', 'Shipping today', 'Opened Yesterday') found in the provided Excel bundle.")
    else:
        st.markdown("Review and assign ownership for the daily prioritized sheets. Edits are synced to the datastore where available.")
        
        w_tabs = st.tabs(list(daily_worklists.keys()))
        for i, (w_name, w_df) in enumerate(daily_worklists.items()):
            with w_tabs[i]:
                st.caption(f"**{w_name}** ({len(w_df)} records)")
                
                edit_df = w_df.copy()
                if "Review" not in edit_df.columns: edit_df["Review"] = False
                if "Owner" not in edit_df.columns: edit_df["Owner"] = ""
                if "Review Comment" not in edit_df.columns: edit_df["Review Comment"] = ""
                if "Needs Review" not in edit_df.columns: edit_df["Needs Review"] = False
                
                col_config = {
                    "Review": st.column_config.CheckboxColumn("Review", default=False),
                    "Owner": st.column_config.TextColumn("Owner"),
                    "Review Comment": st.column_config.TextColumn("Review Comment"),
                    "Needs Review": st.column_config.CheckboxColumn("Needs Review?", default=False)
                }
                disabled_cols = [c for c in edit_df.columns if c not in list(col_config.keys())]
                
                edited = st.data_editor(
                    edit_df,
                    use_container_width=True,
                    num_rows="fixed",
                    disabled=disabled_cols,
                    column_config=col_config,
                    key=f"editor_{w_name}"
                )
                
                if st.button(f"Save '{w_name}' Updates", key=f"btn_save_{w_name}"):
                    changes = edit_df.compare(edited) if hasattr(edit_df, "compare") else edited[edited != edit_df].dropna(how="all")
                    if "conn" in st.session_state and not changes.empty:
                        with st.spinner("Syncing to backend..."):
                            for idx2 in changes.index:
                                row_sel = edited.loc[idx2]
                                order_nr = str(row_sel.get("Order Number", ""))
                                part_nr = str(row_sel.get("Part Number", ""))
                                site_v = str(row_sel.get("Site", ""))
                                upsert_otd_owner(
                                    st.session_state.conn,
                                    order_nr, part_nr, site_v,
                                    owner=row_sel.get("Owner", ""),
                                    reason=row_sel.get("Review Comment", ""),
                                    at_risk=row_sel.get("Needs Review", False)
                                )
                        st.success(f"Updates for {w_name} saved!")
                    elif not changes.empty:
                        st.warning("No backend connection available, but your edits are captured locally in the session.")
                    else:
                        st.info("No modifications detected.")"""
    content = content[:idx] + new_tail
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Replaced to end")
