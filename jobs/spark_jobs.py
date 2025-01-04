import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date
import os
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import avg


def init_spark():
    findspark.init()
    return SparkSession.builder.appName("MergeDatasets").getOrCreate()

def load_dataset(spark, location=False):
    # Carica i dataset mensili
    path = "assets/data/dataset_completo"  
    folders = ["QCLCD201301", "QCLCD201302", "QCLCD201303", "QCLCD201304",
               "QCLCD201305", "QCLCD201306", "QCLCD201307", "QCLCD201308",
               "QCLCD201309", "QCLCD201310", "QCLCD201311", "QCLCD201312"]

    # Carica ciascun dataset in un DataFrame separato
    dataframes = [spark.read.csv(os.path.join(path, folder, folder[5:] + "hourlyM.csv"), header=True, inferSchema=True)
                  for folder in folders]

    # Unisci i DataFrame in uno
    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = merged_df.union(df)

    if location:
        df_stations = spark.read.csv("./assets/data/2013stationM.csv", header=True, inferSchema=True)
        df_stations = df_stations.withColumnRenamed('WBAN', 'wban')
        df_stations = df_stations.select("wban", "Latitude", "Longitude", "StationHeight")
        merged_df = merged_df.join(df_stations, "wban", "inner")

    return merged_df


def create_df_hourly(stations):    
    spark = init_spark()
    merged_df = load_dataset(spark)

    merged_df = merged_df.filter(merged_df["WBAN"].isin(stations))
    
    # Estrai anno, mese, giorno e ora dalle colonne Date e Time
    merged_df = merged_df.withColumn("Datetime", to_date("Date", "dd/MM/yyyy"))
    merged_df = merged_df.withColumn("year", year("Datetime"))
    merged_df = merged_df.withColumn("month", month("Datetime"))
    merged_df = merged_df.withColumn("day", dayofmonth("Datetime"))
    
    features = ["Altimeter", "DewPointCelsius", "DryBulbCelsius", "RelativeHumidity", "SeaLevelPressure", "StationPressure", "Visibility", "WetBulbCelsius", "WindDirection", "WindSpeed"]

    agg_exprs = [avg(col(colonna)).alias(colonna) for colonna in features]
    result = merged_df.groupBy("Datetime", "Date", "year", "month", "day", "Time").agg(*agg_exprs)

    df_ordinato = result.orderBy(col("year"),col("month"),col("day"), col("Time"))

    df_pandas = df_ordinato.toPandas()
    
    spark.stop()
    
    return df_pandas

def clustering(k,location=True):
    spark = init_spark()
    merged_df = load_dataset(spark, location)
    features = ["DewPointCelsius", "DryBulbCelsius", "RelativeHumidity", "Visibility", "WetBulbCelsius", "WindDirection", "WindSpeed"]
    if location:
        features += ["Latitude", "Longitude", "StationHeight"]
    df_senza_null = merged_df.na.drop(subset=features)
    agg_exprs = [avg(col(colonna)).alias(colonna) for colonna in features]
    result = df_senza_null.groupBy("wban").agg(*agg_exprs)
    
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df = assembler.transform(result)

    kmeans = KMeans().setK(k)
    model = kmeans.fit(df)

    clustered_data = model.transform(df)
    clustered_data_pandas = clustered_data.toPandas()
    clustered_data_pandas.rename(columns={'wban':'WBAN'}, inplace=True)

    spark.stop()
    
    return clustered_data_pandas