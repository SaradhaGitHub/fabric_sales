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

cleaning_date='2026-06-11'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
abfs_brnz_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/bronze_sales'
df=spark.read.format('delta').load(abfs_brnz_tbl_path).filter(col('processing_date')==str(cleaning_date))
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_rmv_dupli=df.drop_duplicates()
df_drp_nullrec=df_rmv_dupli.dropna(subset=['Order_ID','Customer_ID'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_add_nwcolmn=df_drp_nullrec.withColumn('Delivery_Days',(col('Ship_date')-col('Order_Date')).cast('int'))
df_add_prftmrgn=df_add_nwcolmn.withColumn('Profit_Margin',col('Profit')/col('Sales'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_add_prftmrgn.createOrReplaceTempView('tmpvw_silvr_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from tmpvw_silvr_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

v_silver_table=f"""CREATE TABLE IF NOT EXISTS SILVER_SALES(
        Row_ID string,
        Order_ID string,
        Order_Date date,
        Ship_Date date,
        Ship_Mode string,
        Customer_ID string,
        Customer_Name string,
        Segment string,
        Postal_Code string,
        City string,
        State string,
        Country string,
        Region string,
        Market string,
        Product_ID string,
        Category string,
        Sub_Category string,
        Product_Name string,
        Sales double,
        Quantity int,
        Discount double,
        Profit double,
        Shipping_Cost double,
        Order_Priority string,
        Month string,
        Year string,
        processing_date date,
        Delivery_Days int,
        Profit_Margin double
        )"""

spark.sql(v_silver_table)       

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


abfs_silvr_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/silver_sales'
try:
    spark.read.format('delta').load(abfs_silvr_tbl_path).createOrReplaceTempView('tmpvw_silvr_sales')
except: 
    v_silver_table=f"""CREATE TABLE IF NOT EXISTS SILVER_SALES(
        Row_ID string,
        Order_ID string,
        Order_Date date,
        Ship_Date date,
        Ship_Mode string,
        Customer_ID string,
        Customer_Name string,
        Segment string,
        Postal_Code string,
        City string,
        State string,
        Country string,
        Region string,
        Market string,
        Product_ID string,
        Category string,
        Sub_Category string,
        Product_Name string,
        Sales double,
        Quantity int,
        Discount double,
        Profit double,
        Shipping_Cost double,
        Order_Priority string,
        Month string,
        Year string,
        processing_date date,
        Delivery_Days int,
        Profit_Margin double
        )"""

    spark.sql(v_silver_table)        
    spark.read.format('delta').load(abfs_silvr_tbl_path).createOrReplaceTempView('tmpvw_silvr_sales')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import lit

abfs_silvr_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/silver_sales'


silver_tbl = DeltaTable.forPath(spark, abfs_silvr_tbl_path)
source_df = spark.table("tmpvw_silvr_data").withColumn("processing_date", lit(cleaning_date))

(
    silver_tbl.alias("t")
    .merge(
        source_df.alias("s"),
        "t.Order_ID = s.Order_ID AND t.Customer_ID = s.Customer_ID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
