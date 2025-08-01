"""
Home page for pyFIFOtax Modern UI
"""

import streamlit as st
import pandas as pd


def show():
    """Display the home page"""

    st.title("pyFIFOtax - FIFO Tax Reporting")
    st.markdown("---")

    # Introduction
    st.markdown(
        """    
    Tax reporting tool for German residents with foreign stock transactions.
    Calculate capital gains and losses using FIFO principles.
    
    ### Key Features
    - **Direct broker data import** - No more Excel intermediate files
    - **FIFO calculations** - Automatic first-in-first-out matching
    - **Currency handling** - Automatic EUR conversion with ECB rates
    - **Event management** - View and edit transaction events by type
    - **Automatic reports** - Tax reports generated automatically when data is imported
    """
    )

    # Getting started guide
    st.markdown("---")
    st.markdown(
        """
    ## Getting Started
    
    1. **Import your data** - Use the Import Data page to upload Schwab JSON files
    2. **Review events** - Check the Events pages to verify imported transactions
    3. **Generate reports** - Tax reports are automatically generated and ready in the Reports section
    
    Navigate using the sidebar to access different sections of the application.
    """
    )

    # Disclaimer section
    st.markdown("---")
    st.markdown(
        """
    **Disclaimer**: This tool provides suggestions, not tax advice. Always consult your tax advisor for professional guidance.
    """
    )
