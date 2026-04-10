import pandas as pd
import numpy as np
pd.set_option('display.max_columns',None)
from data_ingestion_to_sql import connection_engine_to_db,upsert_to_db
import logging
import time

logging.basicConfig(filename='logs/master_table_ingestiontosql.logs',
                    level=logging.DEBUG,
                    format="%(asctime)s-%(levelname)s-%(message)s",
                    filemode='a',
                    force=True)
def cust_value_scoring(df_master):
    try:
        cust_total_series = df_master.groupby('customer_id')['gross_revenue'].transform('sum')
        gross_total = df_master['gross_revenue'].sum()

        df_master['cust_value'] = pd.cut(
            cust_total_series, 
            bins=[-float(np.inf), 0.01*gross_total, 0.05*gross_total, float(np.inf)], 
            labels=['low_value', 'mid_value', 'high_value']
        )
        return df_master
    except Exception as e:
        logging.error(e)

def get_cart_abandoner(df:pd.DataFrame)->pd.DataFrame:
    df_master=df
    try:
        df_master['is_purchase_event']=df_master['event_type'].apply(lambda x:int(True) if 'purchase' in x else int(False))
        
        purchase_ratio_table=df_master.groupby(['customer_id'],as_index=False).agg(
        purchase_session_sum=('is_purchase_event','sum'),
        session_count=('session_id','nunique')
        )
        purchase_ratio_table['purchase_ratio']=(purchase_ratio_table['purchase_session_sum']/purchase_ratio_table['session_count'])*100
        ratio_lookup=purchase_ratio_table.set_index('customer_id')['purchase_ratio']
        df_master['purchase_ratio']=df_master['customer_id'].map(ratio_lookup)
        df_master['is_cart_abandoner']=df_master['purchase_ratio'].apply(lambda x:int(False) if x>=5 else int(True))

        df_master.drop(columns=['is_purchase_event'],inplace=True)
    except Exception as e:
        logging.error(e)
    
    return df_master,purchase_ratio_table


def import_tables():
    import_start=time.time()
    try:
        master_engine,main_engine=connection_engine_to_db()
    except Exception as e:
        logging.error(e)
        raise
    master_table_query='''
    with transactionsdetails AS
    (
        select *,cast(timestamp as date) as transaction_date from transactions
    ),

    customerdetails as
    (
        select * from customers
    ),

    productdetails as
    (
        select * from products
    ),

    camapigndetails as
    (
        select * from campaigns
    ),

    eventdetails as
    (
        select *,cast(timestamp as date) as event_date from events
    )

    select
    ed.event_id,
    ed.event_type,
    ed.customer_id,
    ed.session_id,
    ed.product_id,
    ed.device_type,
    ed.traffic_source,
    ed.campaign_id,
    ed.page_category,
    ed.session_duration_sec,
    ed.experiment_group,
    ed.event_date,

    crd.country,
    crd.age,
    crd.gender,
    crd.loyalty_tier,
    crd.acquisition_channel as acquisition_source,

    pd.category as product_category,
    pd.brand as product_brand,
    pd.base_price as product_base_price,

    cmd.channel as campaign_source,
    cmd.objective as campaign_goal,
    cmd.target_segment as target_customers,

    td.transaction_id,
    td.transaction_date,
    td.customer_id as td_customer_id,
    td.product_id as td_product_id,
    td.quantity,
    td.discount_applied,
    td.gross_revenue,
    td.campaign_id as td_campaign_id


    from eventdetails ed 
    left join customerdetails crd on ed.customer_id=crd.customer_id 
    left join productdetails pd on ed.product_id=pd.product_id
    left join camapigndetails cmd on ed.campaign_id=cmd.campaign_id
    left join transactionsdetails td on ed.customer_id=td.customer_id and
    ed.product_id=td.product_id and ed.event_date=td.transaction_date
    '''
    try:
        df_master=pd.read_sql_query(master_table_query,main_engine)
    except Exception as e:
        logging.error(e)
        raise
    import_end=time.time()
    total_importing_time=(import_end-import_start)/60
    logging.info(f"Total_importation_time:{total_importing_time}")
    return df_master

def data_cleaning():
    cleaning_start=time.time()
    try:
        df_master=import_tables()
    except Exception as e:
        logging.error(e)
        raise
    try:
        df_master.columns=df_master.columns.str.strip().str.lower().str.replace(' ','_')
        df_master.loc[df_master['td_customer_id'].isna(),'td_customer_id']=df_master[df_master['td_customer_id'].isna()]['customer_id']
        df_master.loc[df_master['campaign_source'].isna(),'campaign_source']=df_master[df_master['campaign_source'].isna()]['traffic_source']
        df_master.loc[df_master['td_campaign_id'].isna(),'td_campaign_id']=df_master[df_master['td_campaign_id'].isna()]['campaign_id']
        df_master.loc[(df_master['quantity'].isna()),['quantity','discount_applied','gross_revenue']]=0
        df_master.loc[df_master['transaction_id'].isna(),'transaction_id']=-1
        df_master.loc[df_master['transaction_date'].isna(),'transaction_date']='1900-01-01'
        events=df_master['event_type'].unique().tolist()
        for event in events:
            mode_val=df_master[(df_master['device_type'].notna())&(df_master['event_type']==event)]['device_type'].mode()[0]
            mask=(df_master['device_type'].isna())&(df_master['event_type']==event)
            df_master.loc[mask,'device_type']=mode_val

        df_master.loc[df_master['campaign_id']==0,'campaign_goal']=df_master[df_master['campaign_id']==0]['traffic_source']

        df_master.loc[(df_master['product_id'].isna())&(df_master['event_type']=='bounce'),['product_id','product_base_price','td_product_id']]=-1
        df_master.loc[(df_master['product_brand'].isna())&(df_master['event_type']=='bounce'),['product_brand','product_category']]=str('customer_bounced')
        df_master.loc[(df_master['product_id'].isna())&(df_master['event_type']=='purchase'),['product_id','product_base_price','td_product_id']]=-1
        df_master.loc[(df_master['product_brand'].isna())&(df_master['event_type']=='purchase'),['product_brand','product_category',]]=str('untracked')

        df_master.loc[df_master['target_customers'].isna(),'target_customers']='organic_customers'
        df_master.loc[(df_master['transaction_id']==-1)&(df_master['td_product_id'].isna()),'td_product_id']=-1
        df_master['event_date']=pd.to_datetime(df_master['event_date'],format='%Y-%m-%d')
        df_master['transaction_date']=pd.to_datetime(df_master['transaction_date'],format='%Y-%m-%d')

        cat_cols=df_master.select_dtypes(exclude=['number','datetime']).columns.to_list()
        for col in cat_cols:
            df_master[col]=df_master[col].str.lower().str.strip().astype('object')

        cat_cols=df_master.select_dtypes(include=['str','object']).columns.to_list()
        for col in cat_cols:
            unique_count=df_master[col].nunique()
            static_threshold=200
            if unique_count<=static_threshold:
                df_master[col]=df_master[col].astype('category')
            else:
                df_master[col]=df_master[col].astype('object')
        
        
    except Exception as e:
        logging.error(e)

    cleaning_end=time.time()
    total_cleaning_time=(cleaning_end-cleaning_start)/60
    logging.info(f"Total Cleaning Time : {total_cleaning_time}")
    if df_master.isna().sum().values.sum()==0:
        return df_master
    else:
        logging.error(f'Master table is not cleaned! Found {df_master.isna().sum()}')
        raise ValueError(f'Master table is not cleaned! Found {df_master.isna().sum()}')


def feature_engineer():
    df_master=data_cleaning()
    df_master['session_type']=pd.cut(df_master['session_duration_sec'],bins=[0,10,60,float(np.inf)],labels=['Bouncer','Browser','Engaged'])

    df_master['event_day_num']=df_master['event_date'].dt.day_of_week
    df_master['event_is_weekend']=df_master['event_day_num'].isin([5,6]).astype(int)

    df_master['trans_day_num']=df_master[df_master['transaction_date']!=pd.Timestamp('1900-01-01')]['transaction_date'].dt.day_of_week
    df_master.loc[df_master['trans_day_num'].isna(),'trans_day_num']=-1
    df_master['trans_is_weekend']=df_master['trans_day_num'].apply(lambda x:int(True) if x in [5,6]  else (-1 if x in [-1] else int(False)))

    df_master=cust_value_scoring(df_master)

    df_master,_=get_cart_abandoner(df_master)

    return df_master


def ingest_clean_aggregated_to_db():
    master_ingestion_start=time.time()
    try:
        master_engine,main_engine=connection_engine_to_db()

        master=feature_engineer()
        upsert_to_db(df=master,table_name='master',engine=main_engine,primary_key='event_id')
        
    except Exception as e:
        logging.error(e)
        raise
    master_ingestion_end=time.time()
    master_ingestion_total_time=(master_ingestion_end-master_ingestion_start)/60
    logging.info(f"Total Master Table Ingestion time : {master_ingestion_total_time} minutes")

if __name__=="__main__":
        ingest_clean_aggregated_to_db()
    