# -*- coding: utf-8 -*-

import configparser
import os
import time
from numpy import empty
import streamlit as st
import pandas as pd

from typing import Any

from streamlit.state.session_state import Value
from kafka_config import KafkaConfig,DIRECTORY_NAME,SERVER,DefaultConfigFile,ConfigConst

from web_ui_components import UiComponent

PAGE_TITLE = 'Consumer Pannel'
REFRESH_DELAY = 3

# for main option
LOG = 'View Logs'
CONFIG = 'View Configs'
EDIT = 'Change Configs'
MAIN_OPTION = [LOG, CONFIG, EDIT]
OPTION_TITLE = 'Option'

# for sub option
LOG_TITLE = 'Logs'
CONFIG_TITLE = 'Configs'

# form related
MAX_CHAR = 128

# topic form
TOPIC_TITLE = 'Topics'

HIDE_MENU = """
        <style>
        #MainMenu {visibility: hidden;}
        </style>
        """

class WebData:
    def __init__(self) -> None:
        self.config = KafkaConfig()
        self.settings = self.config.get_config()
        self.dir_name = self.settings.get('path')
        self.file_list = self.get_file_list(self.dir_name)
        self.config_list = self.get_file_list(DIRECTORY_NAME)

    def load_data(self, dir_name:str, log_file:str, config:bool=False) -> None:
        if config:
            if log_file == DefaultConfigFile.KAFKA_FILE_NAME:
                datas = self.config.get_config()
            else:
                datas = self.config.get_topic()
        else:
            # for log
            file_path = os.path.join(dir_name,log_file)
            file = open(file_path,"r")
            lines = file.read().splitlines()
            datas = lines[len(lines)-50:len(lines)]
            file.close()

        return datas

    def get_file_list(self,dir:str):
        return [f.name for f in os.scandir(dir) if f.is_file() and f.name[len(f.name)-3:] == 'log' or (f.name[len(f.name)-4:] == 'conf' and 'server' not in f.name)]

    def update_config(self,current_section:str, new_section:str, options:dict) -> None:
        cfp = configparser.ConfigParser()
        cfp.clear()
        cfp.read(self.config.topic_file)
        cfp.remove_section(current_section)
        cfp.add_section(new_section)
        for opt,val in options.items():
            cfp.set(section=new_section,option=opt,value=str(val))

        with open(self.config.topic_file, "w") as write_file:
            cfp.write(write_file)
        st.info('Save Successfull.')

class WebUi:
    def display_log(self, web_data:WebData, log_file:str) -> None:
        delay = st.slider(label='Delay',min_value=1,max_value=30,value=REFRESH_DELAY)
        st.write('Refresh in',delay, 'sec...')
        st.subheader(log_file)
        placeholder = st.empty()
        while True:
            datas = web_data.load_data(web_data.dir_name, log_file)
            placeholder.write(datas)
            time.sleep(delay)

    def display_config(self, web_data:WebData, config_file:str) -> None:
        st.subheader(config_file)
        placeholder = st.empty()
        datas = web_data.load_data(DIRECTORY_NAME, config_file, config=True)
        placeholder.write(datas)

    def server_form(self,filename:str) -> None:
        st.subheader(f'Edit Config for {filename}')
        st.text('This config not not available to change from UI.')

    def save_topic(self,current_section:str, new_sectioin:str, values:dict):
        WebData().update_config(current_section,new_sectioin,values)

    def topic_form(self, topic_name:str, topic:dict) -> None:
        format_timeout = '%d Seconds'
        format_max_records = '%d Records'

        st.subheader(f'Edit Config for Topic -> {topic_name}')

        placeholder = st.empty()
        with placeholder:
            with st.form('edit_topic_form'):
                col1,col2 = st.columns(2)
                with col1:
                    new_topic = st.text_input(label='Topic',max_chars=MAX_CHAR,placeholder='wallet_journal',value=topic_name)
                    group_id = st.text_input(label='Group ID',max_chars=MAX_CHAR,placeholder='erpdev',value=topic.get(ConfigConst.GROUP_ID))
                    endpoint = st.text_input(label='End point',max_chars=MAX_CHAR,placeholder='http://',value=topic.get(ConfigConst.ENDPOINT))
                    timeout = st.slider(label='Timeout',min_value=1,max_value=30,value=topic.get(ConfigConst.TIMEOUT),format=format_timeout)
                    max_records = st.slider(label='Pool Max Records',min_value=5,max_value=1000,value=topic.get(ConfigConst.MAX_RECORDS),step=5,format=format_max_records)
                with col2:
                    user_name = st.text_input(label='User Name',max_chars=MAX_CHAR,placeholder='admin',value=topic.get(ConfigConst.USERNAME))
                    password = st.text_input(label='Password',max_chars=MAX_CHAR,placeholder='letmein',type='password',value=topic.get(ConfigConst.PASSWORD))
                    active = st.selectbox(label='Active',options=['True','False'],index=0 if topic.get(ConfigConst.ACTIVE) else 1)

                submitted = st.form_submit_button(label='Save')
                if submitted:
                    datas = {
                        ConfigConst.TOPIC:new_topic,
                        ConfigConst.GROUP_ID:group_id,
                        ConfigConst.ENDPOINT:endpoint,
                        ConfigConst.TIMEOUT:timeout,
                        ConfigConst.MAX_RECORDS:max_records,
                        ConfigConst.USERNAME:user_name,
                        ConfigConst.PASSWORD:password,
                        ConfigConst.ACTIVE:active
                    }
                    self.save_topic(topic_name,new_topic,datas)

    def topic_option(self):
        topics = KafkaConfig().get_topic()
        topic_list = [key for key in topics]
        topic_choose = UiComponent.side_selection(TOPIC_TITLE, topic_list)
        self.topic_form(topic_choose, topics.get(topic_choose))

    def view_form(self, config_file:str) -> None:
        if config_file == DefaultConfigFile.TOPIC_FILE_NAME:
            self.topic_option()
        else:
            self.server_form(config_file)

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout='wide')
    st.markdown(HIDE_MENU, unsafe_allow_html=True)

    web_data = WebData()
    # build slider bar
    st.sidebar.title(PAGE_TITLE)

    # main option
    main_option = UiComponent.side_selection(OPTION_TITLE, MAIN_OPTION)
    if main_option == LOG:
        log_file = UiComponent.side_option(LOG_TITLE, web_data.file_list)
        WebUi().display_log(web_data, log_file)
    elif main_option == CONFIG:
        config_file = UiComponent.side_option(CONFIG_TITLE,web_data.config_list)
        WebUi().display_config(web_data, config_file)
    elif main_option == EDIT:
        config_file = UiComponent.side_option(CONFIG_TITLE,web_data.config_list)
        WebUi().view_form(config_file)

def test():
    with st.form('Test'):
        max_records = st.slider(label='Pool Max Records',min_value=0,max_value=1000,value=10,step=5)
        st.write('Max Records',max_records)
        st.form_submit_button('Test')

if __name__ == '__main__':
    main()
    # test()