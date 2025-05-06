import streamlit as st
from tabs.bcp_stat import BCPAutomation
# from tabs.ameyo_extraction import ExtractAmey
from tabs.ameyo_sub import ameyo_main

def main():
    tab1, tab2 = st.tabs(["BCP Automation Tool", "Extract to Ameyo"])

    with tab1:
        bcp = BCPAutomation()
        bcp.display()

    with tab2:
        print("This is the second tab")
        ameyo_main()

# if __name__ == "__main__":
#     main()