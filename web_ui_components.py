# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd

class UiComponent:
    def __init__(self) -> None:
        pass

    @staticmethod
    def side_selection(label:str, datas:list, index:int=0) -> str:
        add_selection = st.sidebar.selectbox(label,datas)
        return add_selection

    @staticmethod
    def side_option(label:str, datas:list, index:int=0) -> str:
        choose = st.sidebar.radio(label, datas, index)
        return choose