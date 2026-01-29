import os
import pyodbc
# from dotenv import load_dotenv
# import streamlit as st


def get_sqlserver_connection(server, database, user, password, driver):
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"MARS_Connection=Yes;"
    )
    return pyodbc.connect(conn_str)


