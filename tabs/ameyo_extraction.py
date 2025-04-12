# extract_amey.py
import streamlit as st

class ExtractAmey:
    def display(self):
        st.header("📤 AMEYO - CMS")
        selected_client = st.selectbox("Select Client", options=["Client A", "Client B", "Client C"])
        if selected_client:
            create_btn = st.button("Fetch Data")
            st.success(f"Selected Client: {selected_client}")
            if create_btn:
                with st.status("Creating report...", expanded=True) as status:
                    st.success(f"Data fetched successfully for {selected_client}!") 
                    status.update(label="Report creation completed!", state="complete")
        else:   
            st.warning("Please select a client.")