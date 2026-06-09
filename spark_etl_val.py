# %%
from pyspark.sql import SparkSession 
from pytest import *
from pyspark.sql.functions  import col 
import pytest_check as pc
import pandas as pd

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
def readData(file_name):
    df=spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema','true')\
    .load(f'C:/Users/ADMIN/Documents/{file_name}.txt')
    return df

# %% [markdown]
# # Count Check

# %%
dfs=readData('src_customer')
dft=readData('tgt_customer')

def test_countCheck():
    dfs_c=dfs.count()
    dft_c=dft.count()
    print(f"<<<Count Mismatch! Source: {dfs_c} rows, Target: {dft_c} rows.>>>")
    assert dfs_c != dft_c ,f"Count Mismatch! Source: {dfs_c} rows, Target: {dft_c} rows."

# %%
test_countCheck()

# %% [markdown]
# # Null Check

# %%
def test_null_check():
    null_count=dfs.filter(col('cust_id').isNull() | col('CUST_FIRST_NAME').isNull() ).count()
    print(f"Null count:{null_count}")
    assert null_count==0 ,'Null Data found'

# %%
test_countCheck()

# %% [markdown]
# # Src Dup

# %%
def test_srcDup ():
    dft_dups=dft.groupBy('cust_id').count().filter(col('count') > 1)
    dft_dups.show()
    assert dft_dups.count() !=0 , f'Duplicates Found:{dft_dups}'
    

# %%
test_srcDup()

# %% [markdown]
# # tgt Dups

# %%
def test_tgtDup ():
    dfs_dups=dfs.groupBy('cust_id').count().filter(col('count') > 1)
    dfs_dups.show()
    assert dfs_dups.count() ==0 , f'Duplicates Found:{dfs_dups}'
    

# %%
test_tgtDup()

# %% [markdown]
# # Metadata Check

# %%
from pyspark.sql import Row
def test_metadata():
    df_col=spark.createDataFrame([Row(dfs.columns),Row(dft.columns)])
    assert list(dfs.columns)==list(dft.columns) , 'Metadata mismatch'
    print(f'Metada matched successfully: {df_col}')
    return df_col

# %%
test_metadata()

# %% [markdown]
# # Datatypes Check

# %%
def test_dataTypeVal():
    mis_dtype_src= [col for col in dfs.dtypes if col not in dft.dtypes]
    mis_dtype_tgt= [col for col in dft.dtypes if col not in dfs.dtypes]
    assert list(dfs.dtypes)==list(dft.dtypes),f'Datatypes mismatch:{mis_dtype_src}-->{mis_dtype_tgt}'

# %%
test_dataTypeVal()


