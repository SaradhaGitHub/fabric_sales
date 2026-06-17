# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e148da5c-4c59-4fb4-a776-a581d18d833d",
# META       "default_lakehouse_name": "Sales_LKHS",
# META       "default_lakehouse_workspace_id": "80750940-2c28-4a25-9a5c-33acb55d87e2",
# META       "known_lakehouses": [
# META         {
# META           "id": "e148da5c-4c59-4fb4-a776-a581d18d833d"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

raw_filename='2016 Apr.csv'
landing_date='2026-06-17'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_foldrpath='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Files/Raw'
# abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Files/Landing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_fullpath=f"{raw_foldrpath}/{raw_filename}"
print(raw_fullpath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.csv(path=raw_fullpath,header=True,inferSchema=True)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
if df.count()>1:
    df_new=df.withColumn("LandedDate",lit(landing_date))
    #display (df_new)
    df_new.write.format('csv').option("header","true").mode("append").partitionBy("LandedDate").save("abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Files/Landing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
