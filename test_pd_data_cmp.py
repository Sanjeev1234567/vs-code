# %% [markdown]
# 
import pip
i
# %%
import pandas as pd
import sqlalchemy as sql
from sqlalchemy import create_engine
import oracledb as odb
import pyodbc as pdb
import urllib

# %%
engine1=sql.create_engine("oracle+oracledb://data_user:admin@localhost:1521/?service_name=freepdb1")
ocon=engine1.connect()
query='select DRIVER_ID, USER_ID, VEHICLE_YEAR, RATING, JOIN_DATE, IS_ACTIVE, VEHICLE_MAKE, LICENSE_PLATE from drivers'
dfo=pd.read_sql(query,ocon)

# %%
conns = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 17 for SQL Server};'
                            r'SERVER=localhost\SQLEXPRESS;'
                            r'DATABASE=sample;'
                            r'Trusted_Connection=yes;')
engine=create_engine(f"mssql+pyodbc:///?odbc_connect={conns}")
query='select * from drivers'
dfs=pd.read_sql(query,engine)

# %% [markdown]
# # Approach 1: The Consolidated "Audit Trail" String (Default)

# %%
#dfo.head(3)
#dfo[dfo['driver_id']==1]
pks='driver_id;user_id'
pk=str.split(pks,sep=';')

dfo_dups=dfo[dfo.duplicated(keep=False)]
dfs_dups=dfs[dfs.duplicated(keep=False)]

df_src=dfo.drop_duplicates(keep='first')
df_src['join_date']=df_src['join_date'].astype('string').str[0:10]
df_tgt=dfs.drop_duplicates(keep='first')

dfo_col=set(dfo.columns)
dfs_col=set(dfs.columns)

mis_src=list(dfo_col-dfs_col)
mis_tgt=list(dfs_col-dfo_col)

com_cols=[c for c in df_src.columns if c in dfs_col and c not in pk ]
all_cols=pk+com_cols


# %%
merged= df_src.merge(df_tgt,on=pk,how='outer',suffixes=('_src','_tgt'),indicator=True)
#df_src[df_src['driver_id']==401]
src_only=merged[merged['_merge']=='left_only']
tgt_only=merged[merged['_merge']=='right_only']
comm=merged[merged['_merge']=='both']

# %%
def test_cmp1():
    report_data=[]
    mis_data=[]
    for ind,row in comm.iterrows():
        report_row={pks:row[pks] for pks in pk }
        is_mis=False
        for col in com_cols:
            v1=str(row[f"{col}_src"]).strip() if pd.notnull(row[f"{col}_src"]) else ''
            v2=str(row[f"{col}_tgt"]).strip() if pd.notnull(row[f"{col}_tgt"]) else ''

            if v1 in ['NA','nan','None']: v1=''
            if v2 in ['NA','nan','None']: v2='' 

            report_row[col]=v1 
            if v1==v2:
                report_row[col]=v1   
            else:
                report_row[col]=f"{v1} --> {v2}"
                is_mis=True
                
        report_data.append(report_row)
        if is_mis==True:
            mis_data.append(report_row)
            
    return pd.DataFrame(report_data),pd.DataFrame(mis_data)

# %%
assert test_cmp1()[1],'Data mismatch observed'

# %% [markdown]
# # Approach 2: The Side-by-Side Column Matrix (Pandas Native)

# %%
def test_compare_via_side_by_side():
    pks='driver_id;user_id'
    if isinstance(pks,str):
        pks=[pk.strip() for pk in pks.split(';')]

    dfo_c=dfo.drop_duplicates(subset=pks,keep='first').set_index(keys=pks)
    dfs_c=dfs.drop_duplicates(subset=pks,keep='first').set_index(keys=pks)

    comm_cols =[c for c in dfo_c.columns if c in dfs_c.columns]

    src_aligned,tgt_aligned=dfo_c[com_cols].align(dfs_c[com_cols],join='inner')

    row_mis=~(src_aligned==tgt_aligned).all(axis=1)
    src_mis=src_aligned[row_mis]
    tgt_mis=tgt_aligned[row_mis]

    cmp_matrix=pd.concat([src_mis,tgt_mis],axis=1,keys=['Src','Tgt'])

    return cmp_matrix.reset_index()

# %%
test_compare_via_side_by_side()

# %% [markdown]
# # Approach 3: The Row-by-Row Long Format Audit Log

# %%
def test_row_row_log():
    pks='driver_id,user_id'
    if isinstance(pks,str):
        pks=[pk.strip() for pk in pks.split(',')]

    dfs_c=dfs.drop_duplicates(subset=pks,keep='first').copy()
    dfo_c=dfo.drop_duplicates(subset=pks,keep='first').copy()

    com_cols=[col for col in dfs_c.columns if col in dfo_c and col not in pks]

    merged=dfo_c.merge(dfs_c,how='outer',indicator=True,suffixes=('_src','_tgt'),on=pks)

    only_src=merged[merged['_merge']=='left_only']
    only_tgt=merged[merged['_merge']=='right_only']
    com_rows=merged[merged['_merge']=='both']

    match_rec=[]
    log_records=[]
    for _,row in com_rows.iterrows():
        key_val="-".join([str(row[pk]) for pk in pks])
        report_row={pk:row[pk] for pk in pks}

        for col in com_cols:
            v1=str(row[f"{col}_src"]).strip() if pd.notnull(row[str(f"{col}_src")]) else ''
            v2=str(row[f"{col}_tgt"]).strip() if pd.notnull(row[str(f"{col}_tgt")]) else ''

            if v1 in ['nan','none']: v1=''
            if v2 in ['nan','none']: v2='' 

            if v1==v2:
                report_row[col]=v1
            else:
                log_records.append({
                    'key_iden':key_val,
                    'col_name':col,
                    'src':v1,
                    'tgt':v2
                })
        match_rec.append(report_row)
    return pd.DataFrame(log_records),pd.DataFrame(match_rec)




# %%
test_row_row_log()[1]


