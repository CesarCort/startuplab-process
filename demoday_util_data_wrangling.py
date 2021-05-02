# -*- coding: utf-8 -*-
"""
Created on Wed Apr  7 15:36:32 2021

@author: Owner
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import date

def list_option(df,column):
    options = df[column][df[column]!=""].to_list()
    return options

def iter_pd(df):
    for val in df.columns:
        yield val
    for row in df.to_numpy():
        for val in row:
            if pd.isna(val):
                yield ""
            else:
                yield val

def pandas_to_sheets(pandas_df, sheet, clear = True):
    # Updates all values in a workbook to match a pandas dataframe
    if clear:
        sheet.clear()
    (row, col) = pandas_df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(pandas_df)):
        cell.value = val
    sheet.update_cells(cells)


def multiply(row):    
    result = row["Ponderación Componente"]*row["value"]
    return result

#%% Conexión a Google Sheets
sheet_file = "EVALUACIÓN DEMO DAY 06.04.021"

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         'C:\\Users\\Owner\\Documents\\credential\\cred_mambo.json', scope) # Your json file here

gc = gspread.authorize(credentials)

#%% conexión a data resultados Demoday
#wks_preguntas = gc.open(sheet_file).worksheet("data")
wks_preguntas = gc.open(sheet_file).worksheet("pre_data")

data = wks_preguntas.get_all_values()
headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)
print(df.head(), df.columns, df.shape)

#%% conexión a data ponderaciones
#wks_preguntas = gc.open(sheet_file).worksheet("data")
wks_preguntas = gc.open(sheet_file).worksheet("atributos")

data = wks_preguntas.get_all_values()
headers = data.pop(0)

df_atributo = pd.DataFrame(data, columns=headers)
print(df.head(), df.columns, df.shape)
#%% conexión a data de proyectos
#wks_preguntas = gc.open(sheet_file).worksheet("data")
sheet_file = "Lista de equipos postulantes al Startup Lab UNSAAC"
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         'C:\\Users\\Owner\\Documents\\credential\\cred_mambo.json', scope) # Your json file here
gc_3 = gspread.authorize(credentials)
wks_team = gc_3.open(sheet_file).worksheet("Startup Lab UNSAAC")

data = wks_team.get_all_values()
headers = data.pop(0)

df_team = pd.DataFrame(data, columns=headers)
df_team = df_team.iloc[:,[1,2,3,4,5,6,7]].copy()

#%% Data wrangling data evaluacion
df = df.drop(columns="Promedio")
data = df.melt(id_vars=['ID', 'Proyecto', 'Descripción',
                    'Sala','Jurado'],
        value_vars=['Grado de innovación de la propuesta',
       'Contenido tecnológico de la propuesta',
       'Nivel de consolidación de la propuesta en el mercado',
       'Tamaño de mercado y viabbilidad de modelo de negocio',
       'Potencial impacto social y/o medioambiental',
       'Capacidad técnica y/o tecnologicas del equipo fundador',
       'Capacidad comercial y empresarial del equipo fundador',
       'Respaldo y redes para llevar adelante la estrategia',
       'Compromiso y dedicación del equipo fundador'])

#%% Merge
data_complete = data.merge(df_team,left_on="Proyecto",right_on="Nombre del proyecto/startup",how="left")
data_complete["value"] = data_complete["value"].astype(float)
#%%
data_complete = data_complete.merge(df_atributo,left_on="variable",right_on="componente",how="left")
data_complete["Ponderación Componente"] = data_complete["Ponderación Componente"].astype(float)
data_complete["weighted_score"] = data_complete.apply(multiply,axis=1)
data_complete["weighted_score"] = data_complete["weighted_score"].astype(float)
#%%
df_project_jurado = data_complete.groupby(["Proyecto"])["Jurado"].count()/9
df_project_jurado = df_project_jurado.reset_index()
df_project_jurado = df_project_jurado.rename(columns={"Jurado":"jurado_count"})
data_complete = data_complete.merge(df_project_jurado,left_on="Proyecto",right_on="Proyecto",how="left")
data_complete["final_value"] = data_complete['weighted_score']/data_complete['jurado_count']

#%% Update to GS
sheet_file = "EVALUACIÓN DEMO DAY 06.04.021"
wks_preguntas = gc.open(sheet_file).worksheet("result")
pandas_to_sheets(data_complete,wks_preguntas)
