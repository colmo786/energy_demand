"""
    energy.py
    Support rutines for DAGs. Application: electricity demand forecasting.
"""
import traceback
# psycopg2: python package to deal with a postgres database
# pip install psycopg2
import psycopg2
from psycopg2.extras import execute_values

import os

import numpy as np
import pandas as pd
# pip install openpyxl
# pip install xlrd

from datetime import datetime, timedelta
from calendar import monthrange

import urllib.request
import json

# pip install requests
import requests
# pip install beautifulsoup4
from bs4 import BeautifulSoup

import zipfile

# Librería para obtener el usuario que está ejecutando el script
import getpass

import hashlib

#!pip install tensorflow
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler

from keras.models import load_model

##-----------------------------------------------------------------------------------------------------------
## AS far as I understood, it's not good to catch run time failures because the DAG will end up without error.
## SO, let the errors go so Airflow task will end with error and the next task will not execute.
##-----------------------------------------------------------------------------------------------------------
def build_postgres_cnxn(database, host, user, password, port=5432, string_connection=None, verbose=True):
    error_txt = ''
    process_ok = True
    cnxn = None
    cursor = None
    if (not host or not user or not password) and not string_connection:
        process_ok = False
        error_txt = 'ERROR build_postgres_cnxn: Error trying to Build DB connexion: you missed to send host, user or password, or string connection. ' +\
                    ' host: ' + (host if host else 'Missed. ') +\
                    ' user: ' + (user if user else 'Missed.') +\
                    ' password: ' + (password if password else 'Missed.') +\
                    ' String Connection: ' + (string_connection if string_connection else 'Missed.')
        if verbose:
            print(error_txt)
    else:
        if not database:
            if verbose:
                print('WARNING build_postgres_cnxn: no database name provided.')
        try:
            if not string_connection:
                cnxn = psycopg2.connect(database=database, host=host, user=user, password=password, port=port)
            else:
                cnxn = psycopg2.connect(string_connection)
            cursor = cnxn.cursor()
            if verbose:
                print('INFO Module build_postgres_cnxn: DB Connection to host', host, 'Ok')
        except Exception as err:
            process_ok = False
            formatted_lines = traceback.format_exc().splitlines()
            txt = ' '.join(formatted_lines)
            if not string_connection:
                if verbose:
                    print('ERROR build_postgres_cnxn: Error connectig to database host: ' + host + ' user ' +\
                      user + ' port: ' + str(port) +'\n' + txt)
            else:
                if verbose:
                    print('ERROR build_postgres_cnxn: Error connectig to database string_connection: ' + string_connection +'\n' + txt)
    return process_ok, error_txt, cnxn, cursor

def pg_select_to_pandas(cursor, sql_query, verbose=True):
    error_txt = ''
    process_ok = True
    df = pd.DataFrame()
    if (not cursor or not sql_query):
        process_ok = False
        error_txt = 'ERROR pg_select_to_pandas: No cursor or Query sent as parameter. ' +\
              ' cursor: ' + (' received.' if cursor else ' missed,') +\
              ' query: ' + (sql_query if sql_query else ' missed.')
        if verbose:
            print(error_txt)
    else:
        try:
            cursor.execute(sql_query)
            data = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data=data, columns=colnames)
            if verbose:
                print('INFO pg_select_to_pandas: query executed Ok. Number of records returned: ' + str(df.shape[0]))
        except Exception as err:
            process_ok = False
            formatted_lines = traceback.format_exc().splitlines()
            txt = ' '.join(formatted_lines)
            error_txt = 'ERROR pg_select_to_pandas: Error executing query on host: ' + cursor.connection.info.host + ' database ' +\
                  cursor.connection.info.dbname + ' query: ' + sql_query +'\n' + txt
            if verbose:
                print(error_txt)
    return process_ok, error_txt, df

def _url_request_to_pandas(_request, verbose=True):
    error_txt = ''
    process_ok = True
    df = pd.DataFrame()
    if (not _request):
        process_ok = False
        error_txt = 'ERROR api_request_to_pandas: No API statement provided.'
        if verbose:
            print(error_txt)
    else:
        try:
            response = urllib.request.urlopen(_request)
            data = response.read()
            encoding = response.info().get_content_charset('utf-8')
            response.close()
            JSON_object = json.loads(data.decode(encoding))
            df = pd.json_normalize(JSON_object)
            if verbose:
                print('INFO api_request_to_pandas: API request executed Ok. Number of records returned: ' + str(df.shape[0]))
        except Exception as err:
            process_ok = False
            formatted_lines = traceback.format_exc().splitlines()
            txt = ' '.join(formatted_lines)
            error_txt = 'ERROR api_request_to_pandas: Error requesting API: ' + _request +'\n' + txt
    return process_ok, error_txt, df

def is_holiday(date=datetime.now(), verbose=True):
    is_a_holiday = 0
    _request = 'http://nolaborables.com.ar/api/v2/feriados/'+str(date.year)
    process_ok, error_txt, df_holidays = _url_request_to_pandas(_request, verbose)
    if not process_ok:
        if verbose:
            print('ERROR is_holiday. ' + error_txt)
    else:
        is_a_holiday = int(df_holidays[(df_holidays.dia==date.day) & (df_holidays.mes==date.month)].shape[0] > 0)
        if verbose:
            print('INFO is_holiday. URL request for ' + str(date) + ' Ok.')
    return process_ok, error_txt, is_a_holiday

def download_file(url: str, dest_file: str, dest_folder: str, verbose=True):
    bool_raise = False
    error_txt = ''
    try:
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)  # create folder if it does not exist
            #filename = url.replace(" ", "_")  # be careful with file names
        file_path = os.path.join(dest_folder, dest_file)

        r = requests.get(url, stream=True)
        if r.ok:
            if verbose:
                print("saving to", os.path.abspath(file_path))
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        else:  # HTTP status code 4XX/5XX
            bool_raise = True
            error_txt = 'Download Url: ' + url + ' failed: status code {}\n{}'.format(r.status_code, r.text)
    except Exception as err:
        bool_raise = True
        formatted_lines = traceback.format_exc().splitlines()
        txt = '\n'.join([line for line in formatted_lines])
        error_txt = 'Error Downloading & Saiving Report. Url: ' + url + ' To path: ' + file_path + '\n' + txt
    return bool_raise, error_txt

def upsert(table, df, primary_key_cols, cnxn, cursor):
    bool_raise = False
    error_txt = ''
    
    user=getpass.getuser()
    df['create_date'] = datetime.now()
    df['create_user'] = user
    df['update_date'] = datetime.now()
    df['update_user'] = user
    tup = [tuple(r) for r in df.to_numpy()]
        
    cols_to_insert = f'({(", ".join(df.columns.tolist()))})'

    primary_key = f'({(", ".join(primary_key_cols))})'

    tup = [tuple(r) for r in df.to_numpy()]
    
    cols_to_update = [col for col in df.columns.tolist() if col not in primary_key_cols + ['create_user', 'create_date']]
    update_sentence = f'{(", ".join([col+"=EXCLUDED."+col for col in cols_to_update]))}'
    
    sql = 'INSERT INTO ' + table + " " + cols_to_insert + " VALUES %s" + " ON CONFLICT " + primary_key + " DO UPDATE "+\
                        'SET ' + update_sentence + ';'
    try:
        execute_values(cursor, sql, tup)
        cnxn.commit()
        print(f'{df.shape[0]} rows upserted to the {table} table')
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'Upsert - SQL Upsert Error. SQL: ' + '\n' + sql + '\n' + txt
    return bool_raise, error_txt

#*************************************************************
# ROUTINES for DAGs
#*************************************************************
###### Hourly process:
def _get_hourly_demand(date, database, host, user, password, port, verbose=True):
    """ Asks API for hourly data of the day (from PARAM date %Y-%m-%d %H:%M) and upserts hourly_demand table.
        NOTE: demand values sometimes come in Nan. This modules ends with error in these cases.
        NOTE: values for 00:00 Hour comes with the data form the day before! So there is an special treatment for this situation
    """
    error_txt = ''
    process_ok = True
    if not date:
        date=datetime.now()
    process_ok, error_txt, cnxn, cursor = build_postgres_cnxn(database=database, host=host, user=user, password=password, port=port, verbose=verbose)
    if not process_ok:
        if verbose:
            print('ERROR _get_hourly_demand calling subprocess: ' + error_txt)
    else:
        if date.hour == 0:
            # Shoul ask API for the day before
            yesterday = date.date() + timedelta(days=-1)
            _request = 'https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegionByFecha?fecha='+\
                        str(yesterday)+'&id_region=1002'
        else:
            _request = 'https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegionByFecha?fecha='+\
                        str(date.date())+'&id_region=1002'
        process_ok, error_txt, df_demand = _url_request_to_pandas(_request, verbose)
        if not process_ok:
            if verbose:
                print(error_txt)
                cnxn.close()
        elif df_demand.shape[0]==0:
            if verbose:
                print('INFO get_hourly_demand. No data retrieved from API. Date: ' + date.strftime('%Y-%m-%d %H:%M'))
        else:
            df_demand.fecha = pd.to_datetime(df_demand.fecha.astype(str).str[:19], format='%Y-%m-%d %H:%M:%S')
            # As you are executing this process any time, depends on when Airflow is alive, you have to be sure that
            # you retrieve the data by the logical time it should have been triggered. 
            # So you have to filter hours after logical date.hour
            df_demand = df_demand[(df_demand.fecha.dt.minute==0) & (df_demand.fecha <= date.strftime('%Y-%m-%d %H:%M'))]
            # demand values can come as Nan --> exclude those records.
            df_demand.dropna(axis=0, subset=['dem'], inplace=True)
            hours = date.hour
            # As hours start in 1, we should ask directly for hour:
            if df_demand.shape[0]<hours:
                if verbose:
                    print('INFO get_hourly_demand - Data retrieved form API is incomplete. Date: ' + str(date) +\
                        ' Hour: ' + str(hours) + ' # of records Ok (should be == to Hour+1): '+ str(df_demand.shape[0]))
            else:
                df_demand.dem = df_demand.dem.astype(int)
                df_demand['day_of_week'] = df_demand.fecha.dt.dayofweek
                process_ok, error_txt, is_a_holiday = is_holiday(date.date(), verbose=verbose)
                if not process_ok:
                    if verbose:
                        print(error_txt)
                    cnxn.close()
                else:
                    df_demand['is_holiday'] = is_a_holiday
                    upsert_user=getpass.getuser()
                    tup = [tuple(np.append(np.append([1002], r), [upsert_user, datetime.now(), upsert_user, datetime.now()])) \
                            for r in df_demand.to_numpy()]
                    sql = 'INSERT INTO cammesa_db.hourly_demand '+\
                        '(region_code, timestamp, hourly_demand, hourly_temp, day_of_week, is_holiday, create_user, create_date, '+\
                        'update_user, update_date) VALUES %s ON CONFLICT (region_code, timestamp) DO UPDATE '+\
                        'SET hourly_demand=EXCLUDED.hourly_demand, hourly_temp=EXCLUDED.hourly_temp, day_of_week=EXCLUDED.day_of_week, '+\
                        'is_holiday=EXCLUDED.is_holiday, update_user=EXCLUDED.update_user, update_date=EXCLUDED.update_date;'
                    execute_values(cursor, sql, tup)
                    cnxn.commit()
                    print('INFO get_hourly_demand - ' + str(len(tup)) + ' records were upserted. Table cammesa_db.hourly_demand.')
        cnxn.close()
    return process_ok
    
def _calculate_hourly_demand_forecast(timestamp_max, database, host, user, password, port, n_lookback=48, n_forecast=24, verbose=True):
    import os
    print('WORKDIR: ' + os.getcwd())
    error_txt = ''
    process_ok = True
    verbose=True
    process_ok, error_txt, cnxn, cursor = build_postgres_cnxn(database=database, host=host, user=user, password=password, port=port, verbose=verbose)
    if not process_ok:
        if verbose:
            print('ERROR _calculate_hourly_demand_forecast calling subprocess: ' + error_txt)
    else:
        # [1] Get last record with history data: 
        sql_query = """SELECT MAX(timestamp) AS max_timestamp
                    FROM cammesa_db.hourly_demand 
                    WHERE timestamp < '""" + timestamp_max.strftime('%Y-%m-%d %H:%M') + """'"""
        process_ok, error_txt, df_last_time = pg_select_to_pandas(cursor, sql_query, verbose=True)
        if not process_ok:
            if verbose:
                print('ERROR nombre_del_modulo calling subprocess: ' + error_txt)
                cnxn.close()
        else:
            last_timestamp = df_last_time.max_timestamp[0]
            if verbose:
                print('INFO _calculate_hourly_demand_forecast forecast timestamp start: ' +\
                                                                                 timestamp_max.strftime('%Y-%m-%d %H:%M'))
                print('INFO _calculate_hourly_demand_forecast hist timestamp max: ' + last_timestamp.strftime('%Y-%m-%d %H:%M'))
            # [1] Get last n_lookback hours --> to predict next n_forecast hour demand
            sql_query = """SELECT timestamp, hourly_demand, hourly_temp, day_of_week, is_holiday 
                        FROM cammesa_db.hourly_demand 
                        WHERE timestamp >= '""" + (last_timestamp+timedelta(hours=-n_lookback)).strftime('%Y-%m-%d %H:%M') +"'" +\
                    """AND timestamp <= '""" + last_timestamp.strftime('%Y-%m-%d %H:%M') +"'"+\
                    """ORDER BY timestamp"""
        process_ok, error_txt, df_lookback = pg_select_to_pandas(cursor, sql_query, verbose=True)
        if not process_ok:
            if verbose:
                print('ERROR nombre_del_modulo calling subprocess: ' + error_txt)
                cnxn.close()
        elif df_lookback.shape[0] < n_lookback:
            if verbose:
                print('ERROR _calculate_hourly_demand_forecast: retrieved less records than requiered for calculation ' +\
                    'n_lookback='+str(n_lookback) + ' retrieved: '+str(df_lookback.shape[0]))
                cnxn.close()
        else:
            # [2] Prepare Data. Normalize, re shape
            # We are not interested in the date, given that each observation is separated by the same interval (hourly) 
            # We build an univariate dataset:
            df_hist_univar = df_lookback[['hourly_demand']]
            # LSTMs are sensitive to the scale of the input data, specifically when the sigmoid (default) or tanh activation functions are used. 
            # It can be a good practice to rescale the data to the range of 0-to-1, also called normalizing.
            scaler = MinMaxScaler(feature_range=(0, 1))
            y = scaler.fit_transform(df_hist_univar)
            # [3] Generate forecasts
            X_ = y[- n_lookback:]  # last available input sequence
            X_ = X_.reshape(1, n_lookback, 1)
            # Load pre-trained model /opt/airflow/dags
            #model = load_model('/opt/airflow/dags/lstm_24_model.h5')
            model = load_model('/home/colmo/airflow/dags/lstm_24_model.h5')
            Y_ = model.predict(X_).reshape(-1, 1)
            Y_ = scaler.inverse_transform(Y_)
            # [4] Upsert data onto database
            df_future = pd.DataFrame(columns=['timestamp', 'hourly_demand_forecast'])
            df_future['timestamp'] = pd.date_range(start=df_lookback['timestamp'].iloc[-1] + pd.Timedelta(hours=1)
                                                , freq='1H', periods=n_forecast)
            df_future['hourly_demand_forecast'] = Y_.flatten()
            if verbose:
                print('INFO _calculate_hourly_demand_forecast: forecast calculated for next '+str(n_forecast)+' hours. Since: '+\
                    last_timestamp.strftime('%Y-%m-%d %H:%M'))
                df_future.hourly_demand_forecast = df_future.hourly_demand_forecast.astype(int)
                df_future['hourly_temp_forecast'] = np.nan
                df_future['day_of_week'] = df_future.timestamp.dt.dayofweek
                for index, row in df_future.iterrows():
                    process_ok, error_txt, is_a_holiday = is_holiday(row['timestamp'], verbose=False)
                    if not process_ok:
                        if verbose:
                            print('ERROR _calculate_hourly_demand_forecast calling submodule: '+error_txt)
                        cnxn.close()
                        break
                    else:
                        df_future['is_holiday'] = is_a_holiday
                if process_ok:
                    upsert_user=getpass.getuser()
                    tup = [tuple(np.append(np.append([1002], r), [upsert_user, datetime.now(), upsert_user, datetime.now()])) \
                                for r in df_future.to_numpy()]
                    sql = """INSERT INTO cammesa_db.hourly_demand_forecast
                            (region_code, timestamp, hourly_demand_forecast, hourly_temp_forecast, day_of_week, is_holiday
                            , create_user, create_date, update_user, update_date) VALUES %s 
                            ON CONFLICT (region_code, timestamp) DO UPDATE
                            SET hourly_demand_forecast=EXCLUDED.hourly_demand_forecast
                            , hourly_temp_forecast=EXCLUDED.hourly_temp_forecast
                            , day_of_week=EXCLUDED.day_of_week
                            , is_holiday=EXCLUDED.is_holiday, update_user=EXCLUDED.update_user, update_date=EXCLUDED.update_date;
                            """
                    execute_values(cursor, sql, tup)
                    cnxn.commit()
                    print('INFO _calculate_hourly_demand_forecast - ' + str(len(tup)) + ' records were upserted. Table cammesa_db.hourly_demand_forecast.')
    return process_ok, error_txt

def process_demand(file, month):
    bool_raise = False
    error_txt = ''
    df_demand = pd.DataFrame()
    try:
        df_demand = pd.read_excel(file, sheet_name='DEMANDA', header=23, usecols='A:L', decimal=',')
        # Rename columns for ease:
        cols = ['year', 'month', 'agent_id', 'agent_desc', 'agent_dem_type', 'region_desc', 'prov_desc'
                , 'area_categ', 'demand_categ', 'tariff_desc', 'tariff_categ', 'monthly_demand_mwh']
        df_demand.columns = cols
        df_demand = df_demand[df_demand.month==month+'-01']
        # There are agents with leading o trailing spaces on desc, those spaces are removed to avoid build distinc ids on
        # master table
        df_demand['agent_desc'] = df_demand.agent_desc.str.strip()
        df_demand['tariff_id'] = df_demand.apply(lambda x: 
                hashlib.sha1(str.encode(x.tariff_desc.strip().lower()+x.tariff_categ.strip().lower())).hexdigest()[:10], axis=1)
        df_demand.sort_values(by=['month', 'agent_id'], inplace=True)
        df_demand.reset_index(drop=True, inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_demand - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_demand

def process_gen(file, month):
    bool_raise = False
    error_txt = ''
    try:
        df_gen = pd.read_excel(file, sheet_name='GENERACION', header=21, usecols='A:O', decimal=',')
        cols = ['year', 'month', 'machine_code', 'central_id', 'agent_id', 'agent_desc', 'region_desc', 'prov_desc', 'pais'
                , 'machine_type', 'source_gen', 'technology', 'hidraulic_categ', 'region_categ', 'monthly_gen_mwh']
        df_gen.columns = cols
        df_gen = df_gen[df_gen.month==month+'-01']

        df_gen['agent_id'] = df_gen.agent_id.str.strip()
        df_gen['agent_desc'] = df_gen.agent_desc.str.strip()
        df_gen['machine_code'] = df_gen.machine_code.str.strip()
        df_gen.prov_desc.fillna('', inplace=True)

        # only 1 country --> remove this column
        df_gen.drop(columns='pais', inplace=True)

        df_gen.sort_values(by=['month', 'machine_code'], inplace=True)
        df_gen.reset_index(drop=True, inplace=True)

        df_gen['machine_id'] = df_gen.apply(lambda x: 
            hashlib.sha1(str.encode(x.machine_code.strip().lower()+x.machine_type.strip().lower()+x.technology.strip().lower())).hexdigest()[:10], axis=1)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_gen - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_gen

def process_prices(file, month):
    bool_raise = False
    error_txt = ''
    try:
        df_precios = pd.read_excel(file, sheet_name='PRECIOS', header=4, decimal=',') #, usecols='A:CE'
        df_precios.drop(columns=['COMPONENTES GENERALES'], inplace=True)
        df_precios.set_index(keys=['DETALLE'], inplace=True)
        df_precios = df_precios.T
        df_precios.reset_index(inplace=True)
        cols = ['month', 'energia', 'energia_ad', 'sobrecost_comb', 'sobrecost_transit_despacho', 'cargo_demanda_exced_real'
            , 'cta_brasil_abast_MEM', 'compra_conj_MEM'
            , 'pot_despachada', 'pot_serv_asoc', 'pot_res_corto_plzo_serv_res_intantanea', 'pot_res_med_plzo', 'monodico'
            , 'transp_alta_tens_distrib_troncal', 'transp_alta_tens', 'transp_distrib_troncal', 'monodico_transp'
            , 'monodico_ponder_estacional_otr_ingr'
            , 'monodico_ponder_estacional_transp']
        df_precios.columns = cols
        df_precios = df_precios[df_precios.month==month+'-01']
        df_precios.month = df_precios.month.dt.date

        df_precios.sort_values(by=['month'], inplace=True)
        df_precios.reset_index(drop=True, inplace=True)

        #df_precios['carg_dem_exce_cta_brasil_contrat_abas_MEM'].fillna(0, inplace=True)
        df_precios.pot_despachada.fillna(0, inplace=True)
        df_precios.pot_serv_asoc.fillna(0, inplace=True)
        df_precios.compra_conj_MEM.fillna(0, inplace=True)
        df_precios['monodico_ponder_estacional_transp'].fillna(0, inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_prices - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_precios

def process_fuels(file, month):
    bool_raise = False
    error_txt = ''
    try:
        df_fuels = pd.read_excel(file, sheet_name='COMBUSTIBLES', header=21, usecols='A:M', decimal=',')
        df_fuels.drop(columns=['REGION', 'PROVINCIA'], inplace=True)

        cols = ['year', 'month', 'machine_code', 'central_id', 'agent_id', 'agent_desc', 'machine_type', 'source_gen', 'technology'
            , 'combustible_type', 'monthly_consume']
        df_fuels.columns = cols
        df_fuels = df_fuels[df_fuels.month==month+'-01']

        df_fuels['agent_id'] = df_fuels.agent_id.str.strip()
        df_fuels['agent_desc'] = df_fuels.agent_desc.str.strip()
        df_fuels['machine_code'] = df_fuels.machine_code.str.strip()

        df_fuels.sort_values(by=['month', 'machine_code'], inplace=True)
        df_fuels.reset_index(drop=True, inplace=True)

        df_fuels['machine_id'] = df_fuels.apply(lambda x: 
            hashlib.sha1(str.encode(x.machine_code.strip().lower()+x.machine_type.strip().lower()+x.technology.strip().lower())).hexdigest()[:10], axis=1)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_fuels - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_fuels

def process_avail(file, month):
    bool_raise = False
    error_txt = ''
    try:
        df_availability = pd.read_excel(file, sheet_name='DISPONIBILIDAD x CENTRAL', header=21, usecols='A:G', decimal=',')
        cols = ['month', 'central_id', 'agent_id', 'agent_desc', 'technology_desc', 'technology', 'monthly_availability_factor']
        df_availability.columns = cols
        df_availability = df_availability[df_availability.month==month+'-01']

        df_availability['agent_id'] = df_availability.agent_id.str.strip()
        df_availability['central_id'] = df_availability.central_id.str.strip()
        df_availability['year'] = df_availability.month.dt.year

        df_availability.sort_values(by=['month', 'central_id', 'technology'], inplace=True)
        df_availability.reset_index(drop=True, inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_avail - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_availability
    
def process_technologies(df_availability):
    df_technologies = df_availability[['technology', 'technology_desc']].copy().drop_duplicates(subset=['technology', 'technology_desc'])
    df = pd.DataFrame(data=[['HID', 'Hidráulica'], ['EOL', 'Eólica'], ['BIOM', 'Biomasa'], ['MHID', 'MHID'], ['SOL', 'Solar'], ['HR', 'HR']],
        columns=['technology', 'technology_desc'])
    df_technologies = pd.concat([df_technologies, df])
    return df_technologies

def process_impo_expo(file, month):
    bool_raise = False
    error_txt = ''
    try:
        df_import_export = pd.read_excel(file, sheet_name='IMP-EXP', header=27, usecols='A:E', decimal=',')
        cols = ['year', 'month', 'pais', 'import_export_type', 'monthly_energy_mwh']
        df_import_export.columns = cols
        df_import_export = df_import_export[df_import_export.month==month+'-01']
        df_import_export['year'] = df_import_export.month.dt.year

        df_import_export.sort_values(by=['month', 'import_export_type', 'pais'], inplace=True)
        df_import_export.reset_index(drop=True, inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_impo_expo - Error retrieving data from Excel: ' +  file + '\n' + txt
    return bool_raise, error_txt, df_import_export

def process_agents(df_demand, df_gen):
    bool_raise = False
    error_txt = ''
    try:
        # Agents form demand dataset:
        df_ = df_demand.drop_duplicates(subset=['agent_id', 'agent_desc', 'agent_dem_type']).groupby(['agent_id']).\
                agg({'agent_desc': 'count'})
        df_.reset_index(inplace=True)
        # Agents with doble description --> both are concatenated
        df_agentes_doble_desc = df_[df_.agent_desc>1]
        agentes_doble_desc = {}
        for agente in df_agentes_doble_desc.agent_id.tolist():
            df_ = df_demand[df_demand.agent_id==agente][['agent_id', 'agent_desc']].drop_duplicates().copy()
            df_.reset_index(inplace=True)
            if df_.loc[0].agent_desc.replace(' ', '') != df_.loc[1].agent_desc.replace(' ', ''):
                agentes_doble_desc[agente] = df_.loc[0].agent_desc + ' | ' + df_.loc[1].agent_desc
        df_agentes_dem = df_demand[['agent_id', 'agent_desc', 'agent_dem_type']].drop_duplicates(subset=['agent_id']).copy()
        df_agentes_dem.set_index(keys='agent_id', inplace=True)
        for index in agentes_doble_desc:
            df_agentes_dem.at[index, 'agent_desc'] = agentes_doble_desc[index]
        df_agentes_dem.reset_index(inplace=True)

        # agents from generation dataset:
        df_ = df_gen.drop_duplicates(subset=['agent_id', 'agent_desc'], keep='last')\
            .groupby(['agent_id']).agg({'agent_desc': 'count'})
        df_.reset_index(inplace=True)
        df_agentes_doble_desc = df_[df_.agent_desc>1]
        for agente in df_agentes_doble_desc.agent_id.tolist():
            df_ = df_gen[df_gen.agent_id==agente][['agent_id', 'agent_desc']].drop_duplicates().copy()
            df_.reset_index(inplace=True)
            if df_.loc[0].agent_desc.replace(' ', '') != df_.loc[1].agent_desc.replace(' ', ''):
                agentes_doble_desc[agente] = df_.loc[0].agent_desc + ' | ' + df_.loc[1].agent_desc
        df_agentes_gen = df_gen[['agent_id', 'agent_desc']].drop_duplicates(subset=['agent_id'], keep='last').copy()
        df_agentes_gen.set_index(keys='agent_id', inplace=True)
        for index in agentes_doble_desc:
            df_agentes_gen.at[index, 'agent_desc'] = agentes_doble_desc[index]
        df_agentes_gen.reset_index(inplace=True)
        
        # Merge both datasets:
        df_merge = df_agentes_dem.merge(right=df_agentes_gen, how='outer', on='agent_id', suffixes=('_x', '_y'))
        #df_agentes_gen[df_agentes_gen.agent_id.isin(df_merge[df_merge.agent_desc_x.isna()].agent_id.tolist())]

        df_agentes = pd.concat([df_agentes_dem, 
            df_agentes_gen[df_agentes_gen.agent_id.isin(df_merge[df_merge.agent_desc_x.isna()].agent_id.tolist())]])
        df_agentes.reset_index(drop=True, inplace=True)
        df_agentes.agent_dem_type.fillna('', inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_agents - Error processing data.' + '\n' + txt
    return bool_raise, error_txt, df_agentes

def process_tariffs(df_demand):
    bool_raise = False
    error_txt = ''
    try:
        df_tariffs = df_demand[['tariff_desc', 'tariff_categ']].drop_duplicates(subset=['tariff_desc', 'tariff_categ']).\
            sort_values(by='tariff_desc')
        df_tariffs['tariff_id'] = df_tariffs.apply(lambda x: 
            hashlib.sha1(str.encode(x.tariff_desc.strip().lower()+x.tariff_categ.strip().lower())).hexdigest()[:10], axis=1)
        df_tariffs.reset_index(drop=True, inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_tariffs - Error processing data.' + '\n' + txt
    return bool_raise, error_txt, df_tariffs

def process_machines(df_gen):
    bool_raise = False
    error_txt = ''
    try:
        df_machines = df_gen.drop_duplicates(subset=['machine_id'])\
            [['machine_id', 'machine_code', 'machine_type', 'source_gen', 'technology', 'hidraulic_categ']]
        df_machines.reset_index(drop=True, inplace=True)
        df_machines.hidraulic_categ.fillna('', inplace=True)
    except Exception as err:
        formatted_lines = traceback.format_exc().splitlines()
        txt = ' '.join(formatted_lines)
        bool_raise = True
        error_txt = 'process_machines - Error processing data.' + '\n' + txt
    return bool_raise, error_txt, df_machines

###### Daily Process
def _get_monthly_data(date, database, host, user, password, port, verbose=True):
    """ Web scrapping of Cammesa's web to download monthly data (from PARAM date %Y-%m-%d %H:%M) and upserts tables for months
        since max month in database.
    """
    error_txt = ''
    process_ok = True
    if not date:
        date=datetime.now()
    # [1] Connect to database
    process_ok, error_txt, cnxn, cursor = build_postgres_cnxn(database=database, host=host, user=user, password=password
                                        , port=port, verbose=verbose)
    if not process_ok:
        if verbose:
            print('ERROR _get_monthly_data building postgres cnxn: ' + error_txt)
        return process_ok
    # [2] Retrieve last month loaded to database:
    sql_query = """select max(month) as month
                from cammesa_db.monthly_demand
                """
    process_ok, error_txt, df_monthly_demand = pg_select_to_pandas(cursor, sql_query, verbose=True)
    if not process_ok:
        if verbose:
            print('ERROR _get_monthly_data quering for last month on DB. Sql query: ' + sql_query + " " + error_txt)
        return process_ok
    df_monthly_demand.month = pd.to_datetime(df_monthly_demand.month)
    last_month = df_monthly_demand.iloc[0].month
    next_month = datetime(last_month.year, last_month.month, monthrange(last_month.year, last_month.month)[1], 0, 0) +\
        timedelta(days=1)
    # [3] Make a list with the months to process (may be none)
    # Considering DAGs should be able to process any time, we consider the situation when a month is not processed and we have to process
    # a range of months:
    bases_to_download = []
    today = date.date()
    current_month = datetime(today.year, today.month, 1, 0, 0, 0)
    month = next_month
    while month <= current_month:
        bases_to_download.append(month.strftime("%Y-%m")) # format: YYYY-mm
        month = datetime(month.year, month.month, monthrange(month.year, month.month)[1], 0, 0) + timedelta(days=1)
    print('Months to Download: ', bases_to_download)
    # [4] Web scrapping: read Cammesa's web page adn look for dynamics url for each month to process
    url = 'https://cammesaweb.cammesa.com/informe-sintesis-mensual/#'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    files_url_dct = {}
    # links to report to download is asociated to "downloads buttons". Search for the key needed:
    # actual month = 'https://cammesaweb.cammesa.com/download/demanda-mensual'
    # history = 'https://cammesaweb.cammesa.com/download/base-informe-mensual-2016-02'
    links=soup.find_all('a', attrs={'class': 'wpdm-download-link download-on-click btn btn-primary btn-sm'})
    for link in links:
        url = link.get('data-downloadurl')
        for base in bases_to_download:
            #### WARNING month 2022-11 was tagged wrongly as 2021-09, so we must download with a different url:
            if 'base' in  url and ('2021-09-2' if base == '2022-11' else base) in url:
                print(url, 'zip')
                files_url_dct[base] = url
    # [5] Download zip files with monthly report. Save it in /home/airflow/dags/data
    dirpath = os.getcwd()
    data_path = os.path.join(dirpath, 'airflow', 'dags', 'data')
    for base in files_url_dct:
        download_file(url=files_url_dct[base], dest_folder=data_path, dest_file='base_informe_mensual_'+base+'.zip')
    # [6] Unzip and then remove .zip file
    for base in bases_to_download:
        zip_file = os.path.join(data_path, 'base_informe_mensual_'+ base + '.zip')
        if os.path.exists(zip_file):
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(data_path, base.replace('-', '_')))
            ## If file exists, delete it: if os.path.isfile(myfile):
            os.remove(zip_file)
    # If this process founds error, it will continue with the next dataset, because should be prepared for re-processing
    i=0
    bool_raise = False
    for base in bases_to_download:
        if True:
        #if i == 0:
            file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Demanda_INFORME_MENSUAL'
                , 'Demanda Mensual.xlsx')
            # We only check the first file if exists (we asume the rest will also exists):
            if os.path.exists(file):
                ####### DEMAND
                bool_raise, error_txt, df_demand = process_demand(file, base)
                if bool_raise:
                    print(error_txt)
                ####### GENERATION
                file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Oferta_INFORME_MENSUAL'
                    , 'Generación Local Mensual.xlsx')
                bool_raise, error_txt, df_gen = process_gen(file, base)
                if bool_raise:
                    print(error_txt)
                ###### PRICES
                file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Adicionales_INFORME_MENSUAL'
                    , 'Precios Mensuales.xlsx')
                bool_raise, error_txt, df_prices = process_prices(file, base)
                if bool_raise:
                    print(error_txt)
                ###### COMBUSTIBLES
                file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Oferta_INFORME_MENSUAL'
                    , 'Combustibles Mensual.xlsx')
                bool_raise, error_txt, df_fuels = process_fuels(file, base)
                if bool_raise:
                    print(error_txt)
                ###### AVAILABILITY
                file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Oferta_INFORME_MENSUAL'
                    , 'Disponibilidad Mensual.xlsx')
                bool_raise, error_txt, df_availability = process_avail(file, base)
                if bool_raise:
                    print(error_txt)
                ##### TECHNOLOGIES
                df_technologies = process_technologies(df_availability)
                ##### IMPO-EXPO
                file = os.path.join(data_path, base.replace('-', '_'), 'BASE_INFORME_MENSUAL_'+base, 'Bases_Adicionales_INFORME_MENSUAL'
                    , 'Import-Export Mensual.xlsx')
                bool_raise, error_txt, df_impo_expo = process_impo_expo(file, base)
                if bool_raise:
                    print(error_txt)
                ##### AGENTS
                bool_raise, error_txt, df_agents = process_agents(df_demand, df_gen)
                if bool_raise:
                    print(error_txt)
                ##### TARIFFS
                bool_raise, error_txt, df_tariffs = process_tariffs(df_demand)
                if bool_raise:
                    print(error_txt)
                ##### MACHINES
                bool_raise, error_txt, df_machines = process_machines(df_gen)
                if bool_raise:
                    print(error_txt)
                ##### UPSERT DATABASE
                # Thinking that this process may need to be re-executed, instead of insert we use upsert:
                table = 'cammesa_db.monthly_prices'
                df = df_prices.copy()
                primary_key_cols = ['month']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.gen_technologies'
                df = df_technologies.copy()
                primary_key_cols = ['technology']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.monthly_import_export'
                df = df_impo_expo.copy()
                primary_key_cols = ['month', 'pais', 'import_export_type']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.agents'
                df = df_agents.copy()
                primary_key_cols = ['agent_id']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.tariffs'
                df = df_tariffs.copy()
                primary_key_cols = ['tariff_id']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.gen_machines'
                df = df_machines.copy()
                primary_key_cols = ['machine_id']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.monthly_demand'
                df_demand.drop(columns=['agent_desc', 'agent_dem_type', 'tariff_desc', 'tariff_categ'], inplace=True)
                df = df_demand.copy()
                primary_key_cols = ['month', 'agent_id', 'region_desc', 'tariff_id']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.monthly_gen'
                df_gen.drop(columns=['machine_code', 'agent_desc', 'machine_type', 'source_gen', 'technology', 'hidraulic_categ'], inplace=True)
                df = df_gen.copy()
                primary_key_cols = ['month', 'machine_id', 'agent_id']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.monthly_combustibles'
                df_fuels.drop(columns=['machine_code', 'agent_desc', 'machine_type', 'source_gen', 'technology'], inplace=True)
                df = df_fuels.copy()
                primary_key_cols = ['month', 'machine_id', 'agent_id', 'combustible_type']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
                table = 'cammesa_db.monthly_availability'
                df_availability.drop(columns=['agent_desc', 'technology_desc'], inplace=True)
                df = df_availability.copy()
                primary_key_cols = ['month', 'central_id', 'agent_id', 'technology']
                bool_raise, error_txt = upsert(table, df, primary_key_cols, cnxn, cursor)
                if bool_raise:
                    print(error_txt)
        i+=1    
    process_ok = not bool_raise
    cnxn.close()
    return process_ok