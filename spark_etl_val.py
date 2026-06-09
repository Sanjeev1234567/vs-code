# %%
from pyspark.sql import SparkSession 
from pytest import *
from pyspark.sql.functions  import col 

# %% [markdown]
# # Spark Session

# %%
spark=SparkSession.builder\
    .appName('ETL Validations')\
    .getOrCreate()

print(f"Spark URL:{spark.sparkContext.uiWebUrl}")

# %% [markdown]
# # Create dataFrame

# %%
def test_readData(file_name):
    df=spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load(f'C:/Users/ADMIN/Documents/{file_name}.txt')
    return df

# %%
dfs=test_readData('src_customer')
dft=test_readData('tgt_customer')

def test_countCheck():
    dfs_c=dfs.count()
    dft_c=dft.count()
    print(f"<<<Count Mismatch! Source: {dfs_c} rows, Target: {dft_c} rows.>>>")
    assert dfs_c != dft_c ,f"Count Mismatch! Source: {dfs_c} rows, Target: {dft_c} rows."

# %%
def test_null_check():
    null_count=dfs.filter(col('cust_id').isNull() | col('CUST_FIRST_NAME').isNull() ).count()
    print(f"Null count:{null_count}")
    assert null_count==0 ,'Null Data found'


