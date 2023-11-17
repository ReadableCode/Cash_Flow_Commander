# %%
## Running Imports ##

# run with streamlit:
# on linux: streamlit run pkg_streamlit.py
# on windows: python -m streamlit run pkg_streamlit.py

# run and share on local network:
# on linux: streamlit run pkg_streamlit.py --server.port 8501
# on windows: python -m streamlit run pkg_streamlit.py --server.port 8501

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

from config_streamlit import (
    report_dir,
)

from src.utils.display_tools import print_logger, pprint_df, pprint_ls


# %%
## Header ##

st.set_page_config(layout="wide")
st.title("PKG Viewer")


# %%
## Inputs ##

# define columns
col1, col2, col3, col4, col5 = st.columns(5)


# %%
