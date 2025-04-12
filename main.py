import streamlit as st
from tabs.bcp_automation import BCPAutomation
from tabs.ameyo_extraction import ExtractAmey

def main():
    tab1, tab2 = st.tabs(["BCP Automation Tool", "Extract to Ameyo"])

    with tab1:
        bcp = BCPAutomation()
        bcp.display()

    with tab2:
        amey = ExtractAmey()
        amey.display()

if __name__ == "__main__":
    main()