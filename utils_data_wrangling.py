# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 23:17:53 2021

@author: Owner
"""
# Import list

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import date

#%% Function zone
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
    
def extract_text(text):
    text = str(text)
    #nps_text = "¿Qué tan probable es que recomiendes la experiencia de llevar este MÓDULO a tu amigos y/o compañero?"
    try:
        new_text = text.split("[")[1].split("]")[0]
        return new_text
    except:
        return text
    
def nps_label(row):
    if row.Pilar =="NPS":
        if row.score >=9:
            return "Promotor"
        elif row.score>=7 and row.score<9:
            return "Pasivo"
        else:
            return "Detractor"
    else:
        return None
#%% procesamiento seteo
module_list = ["Módulo 1","Módulo 2","Módulo 3",
               "Módulo 4","Módulo 5"]

module = module_list[0]

#%% Conexión a Google Sheets

# Para esta conexion se debe crear una cuenta para la administracion de
# uso de APIs GSuite y esa cuenta debe tener acceso compartido al
# documento a acceder.

sheet_file = "Encuesta de Satisfacción 😍 (respuestas)"

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         'C:\\Users\\Owner\\Documents\\credential\\cred_mambo.json', scope) # Your json file here

gc = gspread.authorize(credentials)

#%% conexión a base de preguntas 

# Respuestas directas de Google Form
wks_answer = gc.open(sheet_file).worksheet("Respuestas de formulario 1")

#Respuestas procesadas de Satisfaccion anteriormentes por nuestro Script.
wks_resultados = gc.open(sheet_file).worksheet("resultados procesados")

#Respuestas procesadas anteriormente de NPS por nuestros Script.
wks_resultados_nps = gc.open(sheet_file).worksheet("resultados procesados_nps")

#%% Read Historic data | resultados_satisfaccion 

historic = wks_resultados.get_all_values()
headers = historic.pop(0)

df_historic = pd.DataFrame(historic, columns=headers)
df_historic.replace("", None, inplace=True)
df_historic.dropna(subset=['index'],inplace=True)

# En caso queramos actualizar un resultado, nos quedamos solo con todo menos el modulo a actualizar.
df_historic = df_historic[df_historic["Módulo a evaluar"]!=module]

print(df_historic.head(), df_historic.columns, df_historic.shape)

#%% Read Historic data  | resultados_NPS

historic_nps = wks_resultados_nps.get_all_values()
headers = historic_nps.pop(0)

df_historic_nps = pd.DataFrame(historic_nps, columns=headers)
df_historic_nps.replace("", None, inplace=True)
df_historic_nps.dropna(subset=['index'],inplace=True)
df_historic_nps = df_historic_nps[df_historic_nps["Módulo a evaluar"]!=module]

print(df_historic_nps.head(), df_historic_nps.columns, df_historic_nps.shape)
#%% Read Google Form data.
data = wks_answer.get_all_values()
headers = data.pop(0)

df_answer = pd.DataFrame(
    data, columns=headers)

# Recuerda que el mismo Google Form se enviaba en los diferentes modulos,
# solo que las respuestas eran cambiadas.
# Entonces solo elegimos quedarnos con el modulo que vamos a procesar en ese
# momento
df_answer = df_answer[df_answer["Módulo a evaluar"]==module]

print(df_answer.head(), df_answer.columns, df_answer.shape)


#%% Conexión a Google Sheets
# Esta es otra conexión a otro Google Sheet con información que
# complementa nuestra táctica y datos de nuestras preguntas.
sheet_file = "Táctica de medición"

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         'C:\\Users\\Owner\\Documents\\credential\\cred_mambo.json', scope) # Your json file here

gc_2 = gspread.authorize(credentials)
wks_tactic = gc_2.open(sheet_file).worksheet("QA - Satisfacción")
data = wks_tactic.get_all_values()
headers = data.pop(0)

df_tactic = pd.DataFrame(data, columns=headers)
# Cambiar el número por el módulo a procesar.
df_tactic = df_tactic[df_tactic["Módulo"]=="4"]

print(df_tactic)
#print(df_preguntas.head(), df_preguntas.columns, df_preguntas.shape)

#%% Conexión a Google Sheets

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
#%% Procesando df_answer

if "index" not in list(df_answer.columns):
    df_answer = df_answer.reset_index()
else:
    pass


df_answer["ID"] = df_answer["index"].astype(str) + "_" + df_answer["Selecciona tu Equipo"].astype(str) + "_" + df_answer["Módulo a evaluar"].astype(str)
list_value_vars_1 = ['Me gustó la organización de la clase [Me gustó la organización de la clase]','Me gustó la organización de la clase [Siento que podré definir mejor el PROBLEMA a resolver con mi emprendimiento gracias e este módulo]','Me gustó la organización de la clase [¡Las herramientas usadas están geniales! (Meet, WhatsApp, Facebook)]',       'Me gustó la organización de la clase [Sentí que tenía respaldo de los Tutores ante cualquier duda o ayude que necesite]','Me gustó la organización de la clase [¡El Contenido de la clase tenía cosas que no conocía y que me han abierto la mente!]', 'Me gustó la organización de la clase [Mi Tutor fue un gran apoyo durante esta clase (Dudas teoricas, técnicas, ayuda con el meet-whatsapp o algún problema)]', 'Me gustó la organización de la clase [El Facilitador tiene un buen dominio de los temas expuestos]','Me gustó la organización de la clase [El Facilitador tiene carisma para llegar a los equipos]', 'Me gustó la organización de la clase [Siento que la clase será muy valiosa para llevar mi emprendimiento al siguiente nivel]','¿Qué tan probable es que recomiendes la experiencia de llevar este MÓDULO a tu amigos y/o compañero?']
list_value_vars_2 = ['¿Que tan de acuerdo te sientes con los siguiente enunciados? [Me gustó la organización de la clase]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que podré definir mejor el PROBLEMA a resolver con mi emprendimiento gracias e este módulo]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡Las herramientas usadas están geniales! (Meet, WhatsApp, Facebook)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Sentí que tenía respaldo de los Tutores ante cualquier duda o ayude que necesite]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡El Contenido de la clase tenía cosas que no conocía y que me han abierto la mente!]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Mi Tutor fue un gran apoyo durante esta clase (Dudas teoricas, técnicas, ayuda con el meet-whatsapp o algún problema)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene un buen dominio de los temas expuestos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene carisma para llegar a los equipos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que ahora tengo MEJORES IDEAS para DEFINIR A MI CLIENTE]',       '¿Qué tan probable es que recomiendes la experiencia de llevar este MÓDULO a tu amigos y/o compañero?']
list_value_vars_3 = ['¿Que tan de acuerdo te sientes con los siguiente enunciados? [Me gustó la organización de la clase]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que podré definir mejor el PROBLEMA a resolver con mi emprendimiento gracias e este módulo]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡Las herramientas usadas están geniales! (Meet, WhatsApp, Facebook)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Sentí que tenía respaldo de los Tutores ante cualquier duda o ayude que necesite]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡El Contenido de la clase tenía cosas que no conocía y que me han abierto la mente!]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Mi Tutor fue un gran apoyo durante esta clase (Dudas teoricas, técnicas, ayuda con el meet-whatsapp o algún problema)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene un buen dominio de los temas expuestos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene carisma para llegar a los equipos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que ahora PUEDO identificar puntos claves de mi MODELO DE NEGOCIO]',       '¿Qué tan probable es que recomiendes la experiencia de llevar este MÓDULO a tu amigos y/o compañero?']
list_value_vars_4 = ['¿Que tan de acuerdo te sientes con los siguiente enunciados? [Me gustó la organización de la clase]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que podré diseñar mi primer MVP-Prototipo sin problemas.]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡Las herramientas usadas están geniales! (Meet, WhatsApp, Facebook)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Sentí que tenía respaldo de los Tutores ante cualquier duda o ayude que necesite]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡El Contenido de la clase tenía cosas que no conocía y que me han abierto la mente!]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Mi Tutor fue un gran apoyo durante esta clase (Dudas teoricas, técnicas, ayuda con el meet-whatsapp o algún problema)]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene un buen dominio de los temas expuestos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene carisma para llegar a los equipos]',       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que ahora PUEDO identificar puntos claves de mi MODELO DE NEGOCIO]',       '¿Qué tan probable es que recomiendes la experiencia de llevar este MÓDULO a tu amigos y/o compañero?']
list_value_vars = ['¿Que tan de acuerdo te sientes con los siguiente enunciados? [Lo aprendido hasta hoy me abrío la mente a mejores formas de desarrollar mi emprendimiento]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El apoyo del facilitador fue clave para llegar hasta esta etapa.]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Mi tutor me apoyo en todo lo necesario para llegar hasta aquí.]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que está experiencia de PITCH me reta a querer mejorar.]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [¡El Contenido de la clase tenía cosas que no conocía y que me han abierto la mente!]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Mi Tutor fue un gran apoyo durante esta clase (Dudas teoricas, técnicas, ayuda con el meet-whatsapp o algún problema)]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene un buen dominio de los temas expuestos]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [El Facilitador tiene carisma para llegar a los equipos]',
       '¿Que tan de acuerdo te sientes con los siguiente enunciados? [Siento que ahora PUEDO identificar puntos claves de mi MODELO DE NEGOCIO]']


list_id_vars= ['index','Marca temporal','Selecciona tu Equipo', 'Módulo a evaluar','¿Eres el líder del equipo?','ID']
df = pd.melt(df_answer,id_vars=list_id_vars,value_vars=list_value_vars,var_name="enunciado",value_name="score")

df["enunciado"] = df["enunciado"].apply(lambda x: extract_text(x))
df_complete = df.merge(df_tactic,right_on=["enunciado"],left_on=["enunciado"],how="right")


# test delete
df_complete = df_complete[df_complete["score"]!=""]
df_complete["score"] = df_complete["score"].astype(str)
df_complete = df_complete[df_complete["score"]!="nan"]
df_complete["score"] = df_complete["score"].astype(int)


#%% NPS Analysis + concat

df_nps = df_complete[df_complete["Pilar"]=="NPS"]
df_nps["NPS_label"] = df_nps.apply(nps_label,axis=1)

df_nps = pd.concat([df_historic_nps,df_nps],ignore_index=True)
df_nps["index"].replace("", None, inplace=True)
df_nps.dropna(subset=['index'],inplace=True)

df_nps["score"] = df_nps["score"].astype(int)
df_nps["Módulo"] = df_nps["Módulo"].astype(int)

#%%
pandas_to_sheets(df_nps ,wks_resultados_nps)
#%%
df_complete = df_complete[df_complete["Pilar"]!="NPS"] # clean df_complete
df_complete = df_complete.merge(df_team,right_on=["Nombre del proyecto/startup"],left_on=["Selecciona tu Equipo"],how="right")

# Concat process resultados 
df_complete = pd.concat([df_complete,df_historic],ignore_index=True)
df_complete["index"].replace("", None, inplace=True)
df_complete.dropna(subset=['index'],inplace=True)

df_complete["score"] = df_complete["score"].astype(int)
df_complete["Módulo"] = df_complete["Módulo"].astype(int)
#%%
#Update Results

pandas_to_sheets(df_complete,wks_resultados)
# Finish Process
#%% Feedback
sheet_file = "Encuesta de Satisfacción 😍 (respuestas)"

wks_feedback = gc.open(sheet_file).worksheet("df_feedback")
data = wks_feedback.get_all_values()
headers = data.pop(0)
df_feedback_historic = pd.DataFrame(data, columns=headers)

df_feedback_historic = df_feedback_historic[df_feedback_historic["Módulo a evaluar"]!=module]

# create new df_feedback
list_feedback = ["¿ Tienes alguna idea/propuesta que nos ayude a mejorar el programa/clase/dinámica?"]
df_feedback = df_answer[list_id_vars+list_feedback]

#Update Feedback
pandas_to_sheets(df_feedback,wks_feedback)




