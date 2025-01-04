import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
from tqdm import tqdm

def preprocessing_stations(file):
    df_stations = pd.read_csv(file, sep="|")
    columns = ['WBAN',  'Name', 'State', 'Location', 
           'Latitude', 'Longitude', 'StationHeight', 
           'Barometer', 'TimeZone']
    df_stations = df_stations[columns]
    # hg to hPa
    df_stations['Barometer'] = np.where(df_stations['Barometer'].notnull(), (df_stations['Barometer'].astype(float) * 33.8639).round(2), df_stations['Barometer'].astype(float))
    # foot to meter
    df_stations['StationHeight'] = np.where(df_stations['StationHeight'].notnull(), (df_stations['StationHeight'].astype(float) * 0.3048).round(2), df_stations['StationHeight'].astype(float))
    
    directory = os.path.dirname(file)
    nuovo_nome_file = os.path.splitext(os.path.basename(file))[0] + 'M.csv'
    new_file = os.path.join(directory, nuovo_nome_file)
    df_stations.to_csv(new_file, index=False)
    
    
def preprocessing_precipitations(file):
    df_precip = pd.read_csv(file, sep=",")
    columns = ['Wban', 'YearMonthDay', 'Hour', 'Precipitation']
    columns_flag = ['PrecipitationFlag']     
    df_precip[columns_flag] = df_precip[columns_flag].replace(['s', 'S'], np.nan)
    df_precip.dropna(subset=columns_flag, inplace=True)
    df_precip = df_precip[columns]
    df_precip.replace('M', np.nan, inplace=True)
    df_precip.replace('  T', 'T', inplace=True)
    df_precip.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    # Pollici to centimetri
    condition = (df_precip['Precipitation'].str.strip() != 'T') & (df_precip['Precipitation'].notnull())
    df_precip['Precipitation'] = np.where(condition, (pd.to_numeric(df_precip['Precipitation'], errors='coerce') * 2.54).round(2), df_precip['Precipitation'])
    # int to data
    df_precip['YearMonthDay'] = df_precip['YearMonthDay'].apply(convert_integer_to_date)
    
    directory = os.path.dirname(file)
    nuovo_nome_file = os.path.splitext(os.path.basename(file))[0] + 'M.csv'
    new_file = os.path.join(directory, nuovo_nome_file)
    df_precip.to_csv(new_file, index=False)
    
    
def convert_integer_to_date(date_int):
    date_str = str(date_int)
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_formatted = date_obj.strftime('%d/%m/%Y')
    return date_formatted
    
def convert_gradi(dir_gradi):
    try:
        gradi = int(dir_gradi)
        return gradi*10
    except ValueError:
        return dir_gradi

def convert_mph_to_kmph(speed_mph):
    try:
        speed_mph = float(speed_mph)
        speed_kmph = speed_mph * 1.60934
        return round(speed_kmph, 2)
    except ValueError:
        return speed_mph

def convert_integer_to_date(date_int):
    date_str = str(date_int)
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_formatted = date_obj.strftime('%d/%m/%Y')
    return date_formatted

def convert_time_string(time_string):
    try:
        hour = int(time_string[:2])
        min = int(time_string[2:])
        hour += min//60
        min = min%60
        ts = str(hour).zfill(2) + str(min).zfill(2)
        time = datetime.strptime(ts, '%H%M').time().strftime('%H:%M')
        return time
    except (TypeError, ValueError):
        return time_string        
    
def preprocessing_daily(file):
    df_daily = pd.read_csv(file, sep=",", )
    columns = ['WBAN', 'YearMonthDay', 'Tmax', 'Tmin', 'Tavg', 'Depart', 'DewPoint', 
            'WetBulb', 'Heat', 'Cool', 'Sunrise', 'Sunset', 'CodeSum', 'Depth', 
            'SnowFall', 'PrecipTotal', 'StnPressure', 'SeaLevel', 
            'ResultDir', 'AvgSpeed']
    columns_flag = ['TmaxFlag', 'TminFlag', 'TavgFlag', 'DepartFlag', 'DewPointFlag', 
                    'WetBulbFlag','HeatFlag', 'CoolFlag', 'SunriseFlag','SunsetFlag', 
                    'CodeSumFlag', 'DepthFlag', 'Water1Flag', 'SnowFallFlag', 'PrecipTotalFlag', 
                    'StnPressureFlag', 'SeaLevelFlag','ResultSpeedFlag', 'ResultDirFlag', 
                    'AvgSpeedFlag','Max5SpeedFlag', 'Max5DirFlag', 'Max2SpeedFlag', 'Max2DirFlag']       
    df_daily[columns_flag] = df_daily[columns_flag].replace(['s', 'S'], np.nan)
    df_daily.dropna(subset=columns_flag, inplace=True)
    df_daily = df_daily[columns]
    
    df_daily.replace('M', np.nan, inplace=True)
    df_daily.replace('  T', 'T', inplace=True)
    df_daily['Sunrise'].replace('-', np.nan, inplace=True)
    df_daily['Sunset'].replace('-', np.nan, inplace=True)
    # df_daily['CodeSum'].replace(' ', np.nan, inplace=True)
    df_daily.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    # Fharenheit to Celsius
    df_daily['Tmax'] = np.where(df_daily['Tmax'].notnull(), ((df_daily['Tmax'].astype(float) - 32) * 5/9).round(2), df_daily['Tmax'].astype(float))
    df_daily['Tmin'] = np.where(df_daily['Tmin'].notnull(), ((df_daily['Tmin'].astype(float) - 32) * 5/9).round(2), df_daily['Tmin'].astype(float))
    df_daily['Tavg'] = np.where(df_daily['Tavg'].notnull(), ((df_daily['Tavg'].astype(float) - 32) * 5/9).round(2), df_daily['Tavg'].astype(float))
    df_daily['Depart'] = np.where(df_daily['Depart'].notnull(), ((df_daily['Depart'].astype(float) - 32) * 5/9).round(2), df_daily['Depart'].astype(float))
    df_daily['DewPoint'] = np.where(df_daily['DewPoint'].notnull(), ((df_daily['DewPoint'].astype(float) - 32) * 5/9).round(2), df_daily['DewPoint'].astype(float))
    df_daily['WetBulb'] = np.where(df_daily['WetBulb'].notnull(), ((df_daily['WetBulb'].astype(float) - 32) * 5/9).round(2), df_daily['WetBulb'].astype(float))
    df_daily['Heat'] = np.where(df_daily['Heat'].notnull(), ((df_daily['Heat'].astype(float) - 32) * 5/9).round(2), df_daily['Heat'].astype(float))
    df_daily['Cool'] = np.where(df_daily['Cool'].notnull(), ((df_daily['Cool'].astype(float) - 32) * 5/9).round(2), df_daily['Cool'].astype(float))
    # Pollici to centimetri
    condition1 = (df_daily['SnowFall'].str.strip() != 'T') & (df_daily['SnowFall'].notnull())
    condition2 = (df_daily['Depth'].str.strip() != 'T') & (df_daily['Depth'].notnull())
    condition3 = (df_daily['PrecipTotal'].str.strip() != 'T') & (df_daily['PrecipTotal'].notnull())
    df_daily['SnowFall'] = np.where(condition1, (pd.to_numeric(df_daily['SnowFall'], errors='coerce') * 2.54).round(2), df_daily['SnowFall'])
    df_daily['Depth'] = np.where(condition2, (pd.to_numeric(df_daily['Depth'], errors='coerce') * 2.54).round(2), df_daily['Depth'])
    df_daily['PrecipTotal'] = np.where(condition3, (pd.to_numeric(df_daily['PrecipTotal'], errors='coerce') * 2.54).round(2), df_daily['PrecipTotal'])
    # hg to hPa
    df_daily['StnPressure'] = np.where(df_daily['StnPressure'].notnull(), (df_daily['StnPressure'].astype(float) * 33.8639).round(2), df_daily['StnPressure'].astype(float))
    df_daily['SeaLevel'] = np.where(df_daily['SeaLevel'].notnull(), (df_daily['SeaLevel'].astype(float) * 33.8639).round(2), df_daily['SeaLevel'].astype(float))
    # gradi*10
    df_daily['ResultDir'] = df_daily['ResultDir'].apply(convert_gradi)
    # mph to kmph
    df_daily['AvgSpeed'] = df_daily['AvgSpeed'].apply(convert_mph_to_kmph)
    # int to data
    df_daily['YearMonthDay'] = df_daily['YearMonthDay'].apply(convert_integer_to_date)
    # string to time
    df_daily['Sunrise'] = df_daily['Sunrise'].apply(convert_time_string)
    df_daily['Sunset'] = df_daily['Sunset'].apply(convert_time_string)
    
    df_daily[['Depth', 'SnowFall', 'PrecipTotal']] = df_daily[['Depth', 'SnowFall', 'PrecipTotal']].fillna(0)
    df_daily[['Depth', 'SnowFall', 'PrecipTotal']] = df_daily[['Depth', 'SnowFall', 'PrecipTotal']].replace('T', 0)
    # df_daily.sort_values(['WBAN', 'YearMonthDay'], inplace=True)
    # df_daily['Tmax'] = df_daily.groupby('WBAN')['Tmax'].apply(lambda x: x.interpolate())
    # df_daily['Tmin'] = df_daily.groupby('WBAN')['Tmin'].apply(lambda x: x.interpolate())
    # df_daily['Tavg'] = df_daily.groupby('WBAN')['Tavg'].apply(lambda x: x.interpolate())
    
    directory = os.path.dirname(file)
    nuovo_nome_file = os.path.splitext(os.path.basename(file))[0] + 'M.csv'
    new_file = os.path.join(directory, nuovo_nome_file)
    df_daily.to_csv(new_file, index=False)
    
def convert_mph_to_kmph(speed_mph):
    try:
        speed_mph = float(speed_mph)
        speed_kmph = speed_mph * 1.60934
        return round(speed_kmph, 2)
    except ValueError:
        return speed_mph

def convert_integer_to_date(date_int):
    date_str = str(date_int)
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_formatted = date_obj.strftime('%d/%m/%Y')
    return date_formatted

def convert_time_int(time_int):
    time_string = str(time_int).zfill(4)
    return datetime.strptime(time_string, '%H%M').time().strftime('%H:%M') 

def convert_visibility_to_km(visibility):
    try:
        miles = float(visibility)#.strip())
        km = miles * 1.60934
        return round(km, 2)
    except ValueError:
        print("ERRORE")
        return visibility
    
def aggrega_minuti(df):
    df["Time"] = pd.to_datetime(df["Time"]).dt.hour
    numeric_columns = df.columns.difference(["WBAN", "Date", "Time", "WeatherType", "SkyCondition"])
    # df_numeric = df.groupby(["WBAN", "Date", "Time"])[numeric_columns].mean().round(2).reset_index()
    df_numeric = df.groupby(["WBAN", "Date", "Time"])[numeric_columns].median().reset_index()
    df_categorical = df.groupby(["WBAN", "Date", "Time"])["WeatherType", "SkyCondition"].apply(lambda group: group.sample(n=1)).reset_index(drop=True)
    grouped_df = pd.concat([df_numeric, df_categorical], axis=1)
    return grouped_df

    
def preprocessing_hourly(file):
    df_hourly = pd.read_csv(file, sep=",", )
    columns = ['WBAN', 'Date', 'Time', 'SkyCondition', 'Visibility', 'WeatherType', 
            'DryBulbCelsius', 'WetBulbCelsius', 'DewPointCelsius', 'RelativeHumidity', 'WindSpeed', 'WindDirection', 
            'StationPressure', 'SeaLevelPressure', 'HourlyPrecip', 'Altimeter']
    columns_flag = ['SkyConditionFlag', 'VisibilityFlag', 'WeatherTypeFlag', 'DryBulbCelsiusFlag', 'WetBulbCelsiusFlag', 
                    'DewPointCelsiusFlag','RelativeHumidityFlag', 'WindSpeedFlag', 'WindDirectionFlag','ValueForWindCharacterFlag', 
                    'StationPressureFlag', 'PressureTendencyFlag', 'PressureChangeFlag', 'SeaLevelPressureFlag', 'RecordTypeFlag', 
                    'HourlyPrecipFlag', 'AltimeterFlag']       
    df_hourly[columns_flag] = df_hourly[columns_flag].replace(['s', 'S'], np.nan)
    
    df_hourly.dropna(subset=columns_flag, inplace=True)
    df_hourly = df_hourly[columns]
    
    df_hourly.replace('M', np.nan, inplace=True)
    df_hourly.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    # df_hourly.replace('  ', np.nan, inplace=True)
    df_hourly.replace('VR ', np.nan, inplace=True)
    # df_hourly.replace('   ', np.nan, inplace=True)
    df_hourly.replace('  T', 'T', inplace=True)
    
    # Celsius in float
    df_hourly['DryBulbCelsius'] = df_hourly['DryBulbCelsius'].astype(float)
    df_hourly['WetBulbCelsius'] = df_hourly['WetBulbCelsius'].astype(float)
    df_hourly['DewPointCelsius'] = df_hourly['DewPointCelsius'].astype(float)
    df_hourly['RelativeHumidity'] = df_hourly['RelativeHumidity'].astype(float)
    df_hourly['WindDirection'] = df_hourly['WindDirection'].astype(float)
    # Pollici to centimetri
    condition = (df_hourly['HourlyPrecip'].str.strip() != 'T') & (df_hourly['HourlyPrecip'].notnull())
    df_hourly['HourlyPrecip'] = np.where(condition, (pd.to_numeric(df_hourly['HourlyPrecip'], errors='coerce') * 2.54).round(2), df_hourly['HourlyPrecip'])
    # hg to hPa
    df_hourly['StationPressure'] = np.where(df_hourly['StationPressure'].notnull(), (df_hourly['StationPressure'].astype(float) * 33.8639).round(2), df_hourly['StationPressure'].astype(float))
    df_hourly['SeaLevelPressure'] = np.where(df_hourly['SeaLevelPressure'].notnull(), (df_hourly['SeaLevelPressure'].astype(float) * 33.8639).round(2), df_hourly['SeaLevelPressure'].astype(float))
    df_hourly['Altimeter'] = np.where(df_hourly['Altimeter'].notnull(), (df_hourly['Altimeter'].astype(float) * 33.8639).round(2), df_hourly['Altimeter'].astype(float))
    # mph to kmph
    df_hourly['WindSpeed'] = df_hourly['WindSpeed'].apply(convert_mph_to_kmph)
    df_hourly['Visibility'] = df_hourly['Visibility'].apply(convert_visibility_to_km)
    # int to data
    df_hourly['Date'] = df_hourly['Date'].apply(convert_integer_to_date)
    # int to time
    df_hourly['Time'] = df_hourly['Time'].apply(convert_time_int)
    df_hourly['Visibility'] = df_hourly['Visibility'].astype(float)
    
    df_hourly = aggrega_minuti(df_hourly)

    directory = os.path.dirname(file)
    nuovo_nome_file = os.path.splitext(os.path.basename(file))[0] + 'M.csv'
    new_file = os.path.join(directory, nuovo_nome_file)
    df_hourly.to_csv(new_file, index=False)


# # PREPROCESSING STATIONS-------------------------------------------------------------------------------------------
# path = "assets/data/dataset_completo"
# folders = ["QCLCD201301", "QCLCD201302", "QCLCD201303", "QCLCD201304", 
#            "QCLCD201305", "QCLCD201306", "QCLCD201307", "QCLCD201308", 
#            "QCLCD201309", "QCLCD201310", "QCLCD201311", "QCLCD201312"]
# for folder in folders:
#     folder_name = os.path.join(path, folder)
#     file = os.path.join(folder_name, folder[5:]+"station.txt")
#     preprocessing_stations(file)
    
# dataframes = [pd.read_csv(os.path.join(os.path.join(path, folder), folder[5:]+"stationM.csv")) for folder in folders]

# #Si prende l'intersezione:
# result_df = dataframes[0].merge(dataframes[1], how='inner')
# for df in dataframes[2:]:
#     result_df = result_df.merge(df, how='inner')
# print(result_df.shape)

# # #unione
# # dataframes_no_duplicates = [df.drop_duplicates(subset="WBAN") for df in dataframes]
# # result_df = pd.concat(dataframes_no_duplicates, ignore_index=True)
# # result_df.drop_duplicates(subset="WBAN", keep="first", inplace=True)
# # print(result_df.shape)

# file = "2013stationM.csv"
# result_df.to_csv(os.path.join(path, file), index=False)
# # --------------------------------------------------------------------------------------------------------------------


# # PREPROCESSING PRECIPITATIONS----------------------------------------------------------------------------------------
# path = "assets/data/dataset_completo"
# folders = ["QCLCD201301", "QCLCD201302", "QCLCD201303", "QCLCD201304", 
#            "QCLCD201305", "QCLCD201306", "QCLCD201307", "QCLCD201308", 
#            "QCLCD201309", "QCLCD201310", "QCLCD201311", "QCLCD201312"]
# for folder in tqdm(folders, desc="Processing files", unit="folder"):
#     folder_name = os.path.join(path, folder)
#     file = os.path.join(folder_name, folder[5:]+"precip.txt")
#     preprocessing_precipitations(file)
# # --------------------------------------------------------------------------------------------------------------------

# # PREPROCESSING DAILY-------------------------------------------------------------------------------------------------
# path = "assets/data/dataset_completo"
# folders = ["QCLCD201301", "QCLCD201302", "QCLCD201303", "QCLCD201304", 
#            "QCLCD201305", "QCLCD201306", "QCLCD201307", "QCLCD201308", 
#            "QCLCD201309", "QCLCD201310", "QCLCD201311", "QCLCD201312"]
# for folder in tqdm(folders, desc="Processing files", unit="folder"):
#     folder_name = os.path.join(path, folder)
#     file = os.path.join(folder_name, folder[5:]+"daily.txt")
#     preprocessing_daily(file)
# # --------------------------------------------------------------------------------------------------------------------


# PREPROCESSING HOURLY-------------------------------------------------------------------------------------------------
# path = "assets/data/dataset_completo"
path = "assets/data/dataset_completo"
folders = ["QCLCD201301", "QCLCD201302", "QCLCD201303", "QCLCD201304", 
           "QCLCD201305", "QCLCD201306", "QCLCD201307", "QCLCD201308", 
           "QCLCD201309", "QCLCD201310", "QCLCD201311", "QCLCD201312"]
for folder in tqdm(folders, desc="Processing files", unit="folder"):
    folder_name = os.path.join(path, folder)
    file = os.path.join(folder_name, folder[5:]+"hourly.txt")
    preprocessing_hourly(file)
# --------------------------------------------------------------------------------------------------------------------


