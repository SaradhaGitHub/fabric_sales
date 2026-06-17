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

bronze_date='2026-06-11'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

landg_foldrpath='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Files/Landing'

partition_filename=f"/LandedDate={bronze_date}"
fullpath=landg_foldrpath+partition_filename
print(fullpath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType,DateType
v_schema=StructType([
 StructField("Row_ID",StringType(),True),
    StructField("Order_ID",StringType(),True),
    StructField("Order_Date",StringType(),True),
    StructField("Ship_Date",StringType(),True),
    StructField("Ship_Mode",StringType(),True), 
     StructField("Customer_ID",StringType(),True),
    StructField("Customer_Name",StringType(),True),
    StructField("Segment",StringType(),True),
    StructField("Postal_Code",StringType(),True),
    StructField("City",StringType(),True),
     StructField("State",StringType(),True),
    StructField("Country",StringType(),True),
    StructField("Region",StringType(),True),
    StructField("Market",StringType(),True),
    StructField("Product_ID",StringType(),True),
     StructField("Category",StringType(),True),
    StructField("Sub_Category",StringType(),True),
    StructField("Product_Name",StringType(),True),
    StructField("Sales",DoubleType(),True),
    StructField("Quantity",IntegerType(),True),
     StructField("Discount",DoubleType(),True), 
      StructField("Profit",DoubleType(),True),
    StructField("Shipping_Cost",DoubleType(),True),
    StructField("Order_Priority",StringType(),True),
    StructField("Month",StringType(),True),
     StructField("Year",StringType(),True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, col, coalesce

# Step 1: Read the CSV using your string schema
df = (
    spark.read.format("csv")
        .option("header", True)
        .schema(v_schema)
        .load(fullpath)
)

# Enable legacy parser
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Step 2: Convert the string dates to proper DateType
df = (
    df.withColumn(
        "Order_Date",
        coalesce(
            to_date(col("Order_Date"), "EEEE, MMMM dd, yyyy"),
            to_date(col("Order_Date"), "EEEE, MMMM d, yyyy")
        )
    )
    .withColumn(
        "Ship_Date",
        coalesce(
            to_date(col("Ship_Date"), "EEEE, MMMM dd, yyyy"),
            to_date(col("Ship_Date"), "EEEE, MMMM d, yyyy")
        )
    )
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView('tmpvw_brnz_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM tmpvw_brnz_data;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


abfs_brnz_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/bronze_sales'
try:
    spark.read.format('delta').load(abfs_brnz_tbl_path).createOrReplaceTempView('tmpvw_brnz_sales')
except:
    v_bronze_table=f"""CREATE TABLE IF NOT EXISTS BRONZE_SALES(
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
        processing_date date
        )"""

    spark.sql(v_bronze_table)
    spark.read.format('delta').load('abfs_brnz_tbl_path').createOrReplaceTempView('tmpvw_BRONZE_SALES')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import lit

abfs_brnz_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/bronze_sales'

bronze_tbl = DeltaTable.forPath(spark, abfs_brnz_tbl_path)
source_df = spark.table("tmpvw_brnz_data").withColumn("processing_date", lit(bronze_date)).dropDuplicates(["Order_ID", "Customer_ID"])

(
    bronze_tbl.alias("t")
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

# MARKDOWN ********************

# sql_stmnt=f"""MERGE INTO bronze_sales AS TARGET
#               USING tmpvw_brnz_data AS SOURCE  
#               ON target.Order_ID=source.Order_ID and target.Customer_ID=source.Customer_ID
# 
#               WHEN MATCHED THEN
#               UPDATE SET 
#                 target.Row_ID = source.Row_ID,
#                 target.Order_ID = source.Order_ID,
#                 target.Order_Date = source.Order_Date,
#                 target.Ship_Date = source.Ship_Date,
#                 target.Ship_Mode = source.Ship_Mode,
#                 target.Customer_ID = source.Customer_ID,
#                 target.Customer_Name = source.Customer_Name,
#                 target.Segment = source.Segment,
#                 target.Postal_Code = source.Postal_Code,
#                 target.City = source.City,
#                 target.State = source.State,
#                 target.Country = source.Country,
#                 target.Region = source.Region,
#                 target.Market = source.Market,
#                 target.Product_ID = source.Product_ID,
#                 target.Category = source.Category,
#                 target.Sub_Category = source.Sub_Category,
#                 target.Product_Name = source.Product_Name,
#                 target.Sales = source.Sales,
#                 target.Quantity = source.Quantity,
#                 target.Discount = source.Discount,
#                 target.Profit = source.Profit,
#                 target.Shipping_Cost = source.Shipping_Cost,
#                 target.Order_Priority = source.Order_Priority,
#                 target.Month = source.Month,
#                 target.Year = source.Year,
#                 target.processing_date='{bronze_date}'
# 
#               WHEN NOT MATCHED THEN
#               INSERT (
#                 Row_ID ,
#                 Order_ID,
#                 Order_Date,
#                 Ship_Date,
#                 Ship_Mode,
#                 Customer_ID,
#                 Customer_Name,
#                 Segment,
#                 Postal_Code,
#                 City,
#                 State,
#                 Country,
#                 Region,
#                 Market,
#                 Product_ID,
#                 Category,
#                 Sub_Category,
#                 Product_Name,
#                 Sales,
#                 Quantity,
#                 Discount,
#                 Profit,
#                 Shipping_Cost,
#                 Order_Priority,
#                 Month,
#                 Year,
#                 processing_date
#               )
#               VALUES(
#                 source.Row_ID,
#                 source.Order_ID,
#                 source.Order_Date,
#                 source.Ship_Date,
#                 source.Ship_Mode,
#                 source.Customer_ID,
#                 source.Customer_Name,
#                 source.Segment,
#                 source.Postal_Code,
#                 source.City,
#                 source.State,
#                 source.Country,
#                 source.Region,
#                 source.Market,
#                 source.Product_ID,
#                 source.Category,
#                 source.Sub_Category,
#                 source.Product_Name,
#                 source.Sales,
#                 source.Quantity,
#                 source.Discount,
#                 source.Profit,
#                 source.Shipping_Cost,
#                 source.Order_Priority,
#                 source.Month,
#                 source.Year,
#                 '{bronze_date}'
#               )"""
# spark.sql(sql_stmnt).show()              

