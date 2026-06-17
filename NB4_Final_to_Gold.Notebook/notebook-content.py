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

final_date='2026-06-11'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

abfs_silvr_tbl_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/silver_sales'
df=spark.read.format('delta').load(abfs_silvr_tbl_path).filter(col('processing_date')==str(final_date))
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType,DoubleType
from delta.tables import DeltaTable
dim_custmr_schema=StructType([
        StructField('Customer_ID',StringType(),True),
        StructField('Customer_Name',StringType(),True),
        StructField('Segment',StringType(),True),
        StructField('Postal_Code',StringType(),True),
        StructField('City',StringType(),True),
        StructField('State',StringType(),True),
        StructField('Country',StringType(),True),
        StructField('Region',StringType(),True),
        StructField('Market',StringType(),True)
    ])
DeltaTable.createIfNotExists(spark).tableName('Dim_Customer').addColumns(dim_custmr_schema).execute()    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product_schema=StructType([
        StructField('Product_ID',StringType(),True),
        StructField('Product_Name',StringType(),True),
        StructField('Category',StringType(),True),
        StructField('Sub_Category',StringType(),True)
    ])
DeltaTable.createIfNotExists(spark).tableName('Dim_Product').addColumns(dim_product_schema).execute() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_custmr_schema=StructType([
        StructField('Row_ID',StringType(),True),
        StructField('Order_ID',StringType(),True),
        StructField('Customer_ID',StringType(),True),
        StructField('Product_ID',StringType(),True),
        StructField('Order_Date',DateType(),True),
        StructField('Ship_Date',DateType(),True),
        StructField('Ship_Mode',StringType(),True),
        StructField('Sales',DoubleType(),True),
        StructField('Quantity',IntegerType(),True),
        StructField('Discount',DoubleType(),True),
        StructField('Profit',DoubleType(),True),
        StructField('Shipping_Cost',DoubleType(),True),
        StructField('Order_Priority',StringType(),True),
        StructField('Month',StringType(),True),
        StructField('Year',StringType(),True),
        StructField('Delivery_Days',StringType(),True),
        StructField('Profit_Margin',StringType(),True),
        StructField('Processing_Date',StringType(),True)
    ])
DeltaTable.createIfNotExists(spark).tableName('Fact_Sales').addColumns(fact_custmr_schema).execute() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_custmr=(
    df.select('Customer_ID',
    'Customer_Name',
    'Segment',
    'Postal_Code',
    'City',
    'State',
    'Country',
    'Region',
    'Market'
    ).drop_duplicates()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
import pyspark.sql.functions as F

w = Window.partitionBy("Customer_ID").orderBy(F.col("Customer_ID"))

df_dim_custmr_dedup = (
    df_dim_custmr
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import lit

abfs_dim_custmr_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/dim_customer'


delta_dim_custmr = DeltaTable.forPath(spark, abfs_dim_custmr_path)

delta_dim_custmr.alias('target').merge(df_dim_custmr_dedup.alias('source'),'target.Customer_ID=source.Customer_ID'
).whenMatchedUpdate(set={
    'Customer_Name':'source.Customer_Name',
    'Segment':'source.Segment',
    'Postal_Code':'source.Postal_Code',
    'City':'source.City',
    'State':'source.State',
    'Country':'source.Country',
    'Region':'source.Region',
    'Market':'source.Market'
}).whenNotMatchedInsert(values={
    'Customer_ID':'source.Customer_ID',
    'Customer_Name':'source.Customer_Name',
    'Segment':'source.Segment',
    'Postal_Code':'source.Postal_Code',
    'City':'source.City',
    'State':'source.State',
    'Country':'source.Country',
    'Region':'source.Region',
    'Market':'source.Market'
}).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_df=delta_dim_custmr.history(1)
operation_metrics=history_df.select("OperationMetrics").collect()[0][0]
rows_inserted=operation_metrics.get('numTargetRowsInserted',0)
rows_updated=operation_metrics.get('numTargetRowsUpdated',0)
rows_deleted=operation_metrics.get('numTargetRowsAffected',0)
rows_affected=int(rows_inserted)+int(rows_updated)+int(rows_deleted)

print('Total rows of table: ',delta_dim_custmr.toDF().count())
print(f"Rows Inserted: {rows_inserted}")
print(f"Rows Updated: {rows_updated}")
print(f"Rows Affected: {rows_affected}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_product=(
    df.select('Product_ID',
    'Product_Name',
    'Category',
    'Sub_Category'
    ).drop_duplicates()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
import pyspark.sql.functions as F

w_prod = Window.partitionBy("Product_ID").orderBy(F.col("Product_ID"))

df_dim_product_dedup = (
    df_dim_product
        .withColumn("rn", F.row_number().over(w_prod))
        .filter("rn = 1")
        .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import lit

abfs_dim_product_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/dim_product'


delta_dim_product = DeltaTable.forPath(spark, abfs_dim_product_path)
delta_dim_product.alias('target').merge(df_dim_product_dedup.alias('source'),'target.Product_ID=source.Product_ID'
).whenMatchedUpdate(set={
    'Category':'source.Category',
    'Sub_Category':'source.Sub_Category',
    'Product_Name':'source.Product_Name'
}).whenNotMatchedInsert(values={
    'Product_ID':'source.Product_ID',
    'Category':'source.Category',
    'Sub_Category':'source.Sub_Category',
    'Product_Name':'source.Product_Name'
}).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_sales=(
    df.select('Row_ID',
    'Order_ID',
    'Customer_ID',
    'Product_ID',
    'Order_Date',
    'Ship_Date',
    'Ship_Mode',
        'Sales',
    'Quantity',
    'Discount',
    'Profit',
    'Shipping_Cost',
    'Order_Priority',
        'Month',
    'Year',
    'Delivery_Days',
    'Profit_Margin',
       'processing_date'
    ).drop_duplicates()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.window import Window
import pyspark.sql.functions as F

w_fact = Window.partitionBy("Order_ID", "Product_ID").orderBy(F.col("Order_ID"))

df_fact_dedup = (
    df_fact_sales
        .withColumn("rn", F.row_number().over(w_fact))
        .filter("rn = 1")
        .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import lit

abfs_fact_sales_path='abfss://Fab_Wrkspc_SalesPrjct@onelake.dfs.fabric.microsoft.com/Sales_LKHS.Lakehouse/Tables/dbo/fact_sales'


delta_fact_sales = DeltaTable.forPath(spark, abfs_fact_sales_path)
delta_fact_sales.alias('target').merge(df_fact_dedup.alias('source'),'target.Order_ID=source.Order_ID'
).whenMatchedUpdate(set={
    'Row_ID':'source.Row_ID',
       'Customer_ID':'source.Customer_ID',
    'Product_ID':'source.Product_ID',
    'Order_Date':'source.Order_Date',
    'Ship_Date':'source.Ship_Date',
    'Ship_Mode':'source.Ship_Mode',
    'Sales':'source.Sales',
    'Quantity':'source.Quantity',
    'Discount':'source.Discount',
    'Profit':'source.Profit',
    'Shipping_Cost':'source.Shipping_Cost',
    'Order_Priority':'source.Order_Priority',
        'Month':'source.Month',
    'Year':'source.Year',
    'Delivery_Days':'source.Delivery_Days',
    'Profit_Margin':'source.Profit_Margin',
       'processing_date':'source.processing_date'
}).whenNotMatchedInsert(values={
        'Row_ID':'source.Row_ID',
            'Order_ID':'source.Order_ID',
       'Customer_ID':'source.Customer_ID',
    'Product_ID':'source.Product_ID',
    'Order_Date':'source.Order_Date',
    'Ship_Date':'source.Ship_Date',
    'Ship_Mode':'source.Ship_Mode',
    'Sales':'source.Sales',
    'Quantity':'source.Quantity',
    'Discount':'source.Discount',
    'Profit':'source.Profit',
    'Shipping_Cost':'source.Shipping_Cost',
    'Order_Priority':'source.Order_Priority',
        'Month':'source.Month',
    'Year':'source.Year',
    'Delivery_Days':'source.Delivery_Days',
    'Profit_Margin':'source.Profit_Margin',
       'processing_date':'source.processing_date'
}).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
