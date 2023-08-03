# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 15:05:22 2020

@author: Abhishek Shrivastava
"""

import snowflake.connector
import argparse
import os
import sys
import string
import yaml
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import traceback
import argparse
import textwrap

#################
# main function #
#################

def main(config, filter_cond):
    # logging
    #logging.basicConfig(filename='MetricsValidationRoutine.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = createLogger("Main")
    logger.info("**********************************************")
    logger.info("main function - started")
    t0 = datetime.now()

    # call DataValidation Function
    DataValidation(config)

    #config_file.close()
    t1 = datetime.now()
    logger.info("main function - finished after %s sec" %((t1-t0).total_seconds()))
    logger.info("**********************************************")


# DB Connection, log schema
def open_conn(user, password, account, schema, warehouse, database, role):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        schema=schema,
        warehouse=warehouse,
        database=database,
        role=role
    )
    return ctx


# DB Connection, DWH schema
def open_conn_dwh_schema(user, password, account, schema, warehouse, database, role):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        schema=schema,
        warehouse=warehouse,
        database=database,
        role=role
    )
    return ctx


# DB Connection, DWH schema

def create_eng(user, password, account, schema, warehouse, database, role):
    engine = create_engine(URL(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role=role,
        schema=schema
    ))
    return engine

# grab a logger
def createLogger(loggerName):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(os.getcwd() + '/MetricsValidationRoutine.log')
    #print(handler)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def DataValidation(config):

    logger = createLogger("DataValidationFunction")

    # variables
    sqlMetricConfig = "SELECT * FROM DM_METRICS_CONFIG WHERE IN_EFFECT_FLAG = 1 ;"
    sqlComparisonConfig = "SELECT * FROM DM_COMPARISON_CONFIG WHERE IN_EFFECT_FLAG = 1 ;"

    logger.info("Read environment details - started")
    
    # read config values
    warehouse_name = get_cfg(config, "computing.virtual_wh")
    user = get_cfg(config, "user_info.user")
    password = get_cfg(config, "user_info.password")
    account = get_cfg(config, "user_info.account")
    database = get_cfg(config, "storage.database")
    log_schema = get_cfg(config, "storage.log_schema")
    dwh_schema = get_cfg(config, "storage.dwh_schema")
    role = get_cfg(config, "user_info.role")

    logger.info("Read environment details - finished")
    logger.info("Create db connection - started")

    try:
        # establish db connections
        conn = open_conn(user, password, account, log_schema, warehouse_name, database, role)
        #conn_dwh_schema = open_conn_dwh_schema(user, password, account, dwh_schema, warehouse_name, database, role)
        engine = create_eng(user, password, account, log_schema, warehouse_name, database, role)
    except Exception as ex:
        logger.error("error in db connection %s: %s" %(str(ex), traceback.format_exc()))

    logger.info("Create db connection - finished")
    
    logger.info("Update metrics outcome - started")

    try:
        # read metric config data into dataframe
        df_DM_METRICS_CONFIG = pd.read_sql_query(sqlMetricConfig, conn)

        # iterate over metrics config rows using corresponding dataframe
        for index, row in df_DM_METRICS_CONFIG.iterrows():
            t0 = datetime.now()
            col_for_count_available = None
            col_group_by_available = None

            sql_available_flag = row['SQL_AVAILABLE_FLAG']
            validation_type_key = row['VALIDATION_TYPE_KEY']
            col_for_count_available =  row['COLUMN_TO_GET_COUNT']
            col_group_by_available = row['COLUMNS_GROUP_BY_COND']
            col_whr_cond_available = row['COLUMNS_WHERE_COND']
            param_1 = row['PARAM_1']
            param_2 = row['PARAM_2']

            if col_group_by_available is not None and col_group_by_available!= '':
                col_group_by_available= 1

            if sql_available_flag == 1:
                metric_sql = row['SQL_TEXT']
            elif col_for_count_available is not None and col_for_count_available!= '':
                metric_sql = "SELECT '' GROUP_BY_VAL, " + str(row['COLUMN_TO_GET_COUNT']) + " REC_COUNT FROM " +\
                             str(row['DESTINATION_TBL']) + " replace_where_clause "
            elif col_group_by_available ==1:
                metric_sql = 'SELECT ' + str(row['COLUMNS_GROUP_BY_COND']) + ', ' + \
                             str(row['COLUMNS_GROUP_BY_COND']).replace(",", "|| ', ' ||") + ' GROUP_BY_VAL, '
                if validation_type_key == 2 and param_2 is None:
                    metric_sql = metric_sql + ' COUNT(1) - COUNT(' + str(row['COLUMN_TO_VALIDATE']) + ') '
                elif validation_type_key == 5:
                    metric_sql = metric_sql + str(row['PARAM_1']) + "  (" + str(row['COLUMN_TO_VALIDATE']) + ") "
                else:
                    metric_sql = metric_sql + ' COUNT(1) '
                metric_sql = metric_sql + ' REC_COUNT FROM ' + str(row['DESTINATION_TBL']) + ' replace_where_clause ' + \
                             ' GROUP BY  ' + str(row['COLUMNS_GROUP_BY_COND'])
            else:
                metric_sql = "SELECT '' GROUP_BY_VAL,  "
                if validation_type_key == 2 and param_2 is not None:
                    my_list = str(row['COLUMN_TO_VALIDATE']).split(",")
                    j=1
                    space="'"
                    rec_count=""
                    main_select="select " + str(row['PARAM_2']).replace(",","||';'||") +"||';'||"
                    column_value="select "+str(row['PARAM_1']) +","
                    for i in my_list:
                        column_value = column_value + str(space)+ str(i) + str(space)+" as c" +str(j) + ", round(sum(case when "+i+" is null then 1 else 0 end)*100/count(*)) as value"+str(j)+","
                        main_select=main_select + " c"+str(j) + "||';'||value" + str(j) +"||';'||"
                        rec_count=rec_count+"value"+str(j)+"+"
                        j+=1
                        inner_query = column_value[:-1]+ " from " +str(row['DESTINATION_TBL']) +" group by "+ str(row['COLUMNS_GROUP_BY_COND']) +" order by " + str(row['COLUMNS_GROUP_BY_COND'])
                    metric_sql = main_select[:-7]+" AS GROUP_BY_VAL, ("+ rec_count[:-1] +")" + " as REC_COUNT from (" + inner_query + ");"
                elif validation_type_key == 5:
                    metric_sql = metric_sql + str(row['PARAM_1'])  + "  (" + str(row['COLUMN_TO_VALIDATE']) + ") "
                else:
                    metric_sql = metric_sql + " COUNT(1) "
                metric_sql = metric_sql + " REC_COUNT FROM " + str(row['DESTINATION_TBL']) + " replace_where_clause "

            if col_whr_cond_available is not None and col_whr_cond_available != '':
                #metric_sql = metric_sql.replace('replace_where_clause', " WHERE " + str(row['COLUMNS_WHERE_COND']) + " = " + "" + str(row['COLUMNS_WHERE_COND_VALUES']) + "")
                metric_sql = metric_sql.replace('replace_where_clause', " WHERE " + str(row['COLUMNS_WHERE_COND']) )
            else:
                metric_sql= metric_sql.replace('replace_where_clause',"")

            #print(metric_sql)
            #execute on targeted schema. If schema is specified in PARAM_1 then ETL Schema otherwise default DWH schema
            """
            if param_1_available is not None and param_1_available!= '':
                conn_dwh_schema.cursor().execute("use schema %s" %(row['PARAM_1']))
                df_DM_METRICS_OUTCOME = pd.read_sql_query(metric_sql, conn_dwh_schema)
            else:
                df_DM_METRICS_OUTCOME = pd.read_sql_query(metric_sql, conn)
            """

            df_DM_METRICS_OUTCOME = pd.read_sql_query(metric_sql, conn)

            # current date in int format
            curr_datetime_int = int(datetime.now().strftime('%Y%m%d'))

            # add additonal columns into dataframe
            df_DM_METRICS_OUTCOME['METRICS_KEY'] = row['DM_METRICS_CONFIG_KEY']
            df_DM_METRICS_OUTCOME['RUN_DATE'] = datetime.now()
            df_DM_METRICS_OUTCOME['RUN_DATE_INT'] = curr_datetime_int
            df_DM_METRICS_OUTCOME['COLUMNS_GROUP_BY_COND'] = row['COLUMNS_GROUP_BY_COND']
            if 'GROUP_BY_VAL' not in df_DM_METRICS_OUTCOME.columns: df_DM_METRICS_OUTCOME['GROUP_BY_VAL'] = ''

            # insert dataframe data into a temp table
            df_DM_METRICS_OUTCOME.to_sql("dm_metrics_outcome_tmp", con=engine, index=False, if_exists="replace")

            # insert data into metrics outcome table from temp table
            metric_result_insert_sql = " INSERT INTO DM_METRICS_OUTCOME(METRICS_KEY, RUN_DATE_INT, RUN_DATE, LEVEL_1, LVL_VALUE_1, METRIC_OUTCOME_VALUE)" \
                                       " SELECT METRICS_KEY, RUN_DATE_INT, RUN_DATE, COLUMNS_GROUP_BY_COND, GROUP_BY_VAL, REC_COUNT FROM dm_metrics_outcome_tmp"

            metric_result_insert_sql = metric_result_insert_sql + " WHERE NOT EXISTS" \
                                                                  "(SELECT METRICS_KEY FROM DM_METRICS_OUTCOME WHERE DM_METRICS_OUTCOME.METRICS_KEY = dm_metrics_outcome_tmp.METRICS_KEY" \
                                                                  " AND DM_METRICS_OUTCOME.RUN_DATE_INT = dm_metrics_outcome_tmp.RUN_DATE_INT" \
                                                                  " AND DM_METRICS_OUTCOME.LVL_VALUE_1 = dm_metrics_outcome_tmp.GROUP_BY_VAL)"

            conn.cursor().execute(metric_result_insert_sql)
            conn.cursor().execute(
                "UPDATE INT_ETL_AUDIT.DM_METRICS_OUTCOME SET YEAR = YEAR(RUN_DATE), MONTH = MONTH (RUN_DATE), DAY = DAY(RUN_DATE) WHERE YEAR IS NULL")
            conn.commit()
            del df_DM_METRICS_OUTCOME
            t1 = datetime.now()
            logger.info("Processed metric key %s after %s sec" %(row['DM_METRICS_CONFIG_KEY'],(t1-t0).total_seconds()) )

    except Exception as ex:
        logger.error("error in metrics outcome block %s: %s" %(str(ex), traceback.format_exc()))

    logger.info("Update metrics outcome - finished")

    logger.info("Update comparison outcome - started")

    try:
        # load metrics outcome into dataframe
        # Down will create subset of it for specific metric key and date in every iteration corresponding to comparison config table record
        df_met_outcome_load = pd.read_sql_query("SELECT * FROM DM_METRICS_OUTCOME;", conn)

        # print(df_met_outcome_load)

        # read comparison config data into dataframe
        df_comp_config = pd.read_sql_query(sqlComparisonConfig, conn)

        # to handle NaN values in TARGET_METRIC_KEY column
        df_comp_config['TARGET_METRIC_KEY'] = df_comp_config['TARGET_METRIC_KEY'].fillna(0)

        # iterate over comparison config rows using corresponding dataframe
        for index, row in df_comp_config.iterrows():
            t0 = datetime.now()
            validation_type_key = row['VALIDATION_TYPE_KEY']
            write_to_log = row['WRITE_TO_LOG']
            tgt_metric_key = row['TARGET_METRIC_KEY']

            #  filter metric outcome dataframe corresponding to source metric key and run date
            df_met_outcome_subset = df_met_outcome_load[df_met_outcome_load['METRICS_KEY'] == row['SOURCE_METRIC_KEY']]
            if df_met_outcome_subset.empty == False: df_met_outcome_subset = df_met_outcome_subset[
                df_met_outcome_load['RUN_DATE_INT'] == curr_datetime_int]

            #  filter metric outcome dataframe corresponding to target metric key and run date
            df_met_outcome_subset_2 = df_met_outcome_load[df_met_outcome_load['METRICS_KEY'] == row['TARGET_METRIC_KEY']]
            if df_met_outcome_subset_2.empty == False: df_met_outcome_subset_2 = df_met_outcome_subset_2[
                df_met_outcome_load['RUN_DATE_INT'] == curr_datetime_int]

            # log remark and flag status as 0 if corresponding metric detail is not available
            if df_met_outcome_subset.empty or( (tgt_metric_key != 0) and df_met_outcome_subset_2.empty):
                comfig_outcome_insert_sql = "MERGE INTO DM_COMPARISON_OUTCOME USING (SELECT * FROM (VALUES (" + str(
                    row['DM_COMPARISON_CONFIG_KEY']) + ", TO_CHAR(CURRENT_DATE,'YYYYMMDD'))) " \
                                                       "  x (DM_COMPARISON_CONFIG_KEY, RUN_DATE_INT)) TEMP_TBL ON " \
                                                       " DM_COMPARISON_OUTCOME.COMPARISON_KEY = TEMP_TBL.DM_COMPARISON_CONFIG_KEY AND DM_COMPARISON_OUTCOME.RUN_DATE_INT = TEMP_TBL.RUN_DATE_INT  " \
                                                       " WHEN NOT MATCHED THEN "

                comfig_outcome_insert_sql = comfig_outcome_insert_sql + "INSERT (COMPARISON_KEY, RUN_DATE_INT, RUN_DATE, STATUS_FLAG, REMARKS) VALUES (" + str(
                    row[
                        'DM_COMPARISON_CONFIG_KEY']) + ", TO_CHAR(CURRENT_DATE,'YYYYMMDD') , CURRENT_DATE(), 0, 'Either metrics key does not exist OR is not in effect.' )"

                # print(comfig_outcome_insert_sql)
                conn.cursor().execute(comfig_outcome_insert_sql)
                conn.commit()
                t1 = datetime.now()
                logger.info("Processed comparison key %s after %s sec" % (row['DM_COMPARISON_CONFIG_KEY'],(t1-t0).total_seconds()))
                continue

            # insert rows in comparison outcome according to write_to_log flag
            if write_to_log == 1:
                varBoolean = 1
                severity = ''
                # logic for validation_type == 1 & i.e DISTINCT_VALUE_COUNT & Duplicate records
                if validation_type_key == 1 or validation_type_key == 3 :
                    for index2, row2 in df_met_outcome_subset.iterrows():
                        if row['FIXED_VALUE_MIN'] != row2['METRIC_OUTCOME_VALUE']:
                            varBoolean = -1

                elif validation_type_key == 2 or validation_type_key == 5:
                    for index2, row2 in df_met_outcome_subset.iterrows():
                        if row['FIXED_VALUE_MIN'] < row2['METRIC_OUTCOME_VALUE'] and row['METRICS_THRESHOLD'] >= row2['METRIC_OUTCOME_VALUE']:
                            varBoolean = -1
                            severity = 'Medium'
                        elif row['METRICS_THRESHOLD'] < row2['METRIC_OUTCOME_VALUE']:
                            varBoolean = -1
                            severity = 'High'
                elif validation_type_key == 4:
                    df_met_outcome_subset_2 = df_met_outcome_subset_2.add_suffix('_TGT')
                    df_met_outcome_subset_2 = df_met_outcome_subset_2.rename(columns={"LVL_VALUE_1_TGT": "LVL_VALUE_1"})
                    df_met_outcome_subset = pd.merge(df_met_outcome_subset, df_met_outcome_subset_2, on=['LVL_VALUE_1'], how='inner')
                    for index2, row2 in df_met_outcome_subset.iterrows():
                        if row2['METRIC_OUTCOME_VALUE'] != row2['METRIC_OUTCOME_VALUE_TGT']:
                            varBoolean = -1

                comp_outcome_insert_sql = "MERGE INTO DM_COMPARISON_OUTCOME USING (SELECT * FROM (VALUES (" + str(
                    row['DM_COMPARISON_CONFIG_KEY']) + ", TO_CHAR(CURRENT_DATE,'YYYYMMDD'))) " \
                                                       "  x (DM_COMPARISON_CONFIG_KEY, RUN_DATE_INT)) TEMP_TBL ON " \
                                                       " DM_COMPARISON_OUTCOME.COMPARISON_KEY = TEMP_TBL.DM_COMPARISON_CONFIG_KEY AND DM_COMPARISON_OUTCOME.RUN_DATE_INT = TEMP_TBL.RUN_DATE_INT  " \
                                                       " WHEN NOT MATCHED THEN "

                comp_outcome_insert_sql = comp_outcome_insert_sql + "INSERT (COMPARISON_KEY, RUN_DATE_INT, RUN_DATE, YEAR, MONTH, DAY, STATUS_FLAG, SEVERITY) VALUES (" + str(
                    row['DM_COMPARISON_CONFIG_KEY']) + ", TO_CHAR(CURRENT_DATE,'YYYYMMDD') , CURRENT_DATE(), YEAR(CURRENT_DATE()), MONTH(CURRENT_DATE()), DAY(CURRENT_DATE()), " + str(
                    varBoolean) + ",'"+ str(severity) + "')"

                #print(comp_outcome_insert_sql)
                conn.cursor().execute(comp_outcome_insert_sql)
                conn.commit()

            else:
                df_met_outcome_subset['FIXED_VALUE_MIN'] = row['FIXED_VALUE_MIN']
                df_met_outcome_subset['METRICS_THRESHOLD'] = row['METRICS_THRESHOLD']
                df_met_outcome_subset['STATUS_FLAG'] = 1
                df_met_outcome_subset['COMPARISON_KEY'] = row['DM_COMPARISON_CONFIG_KEY']
                df_met_outcome_subset['SEVERITY'] = ''

                if validation_type_key == 1 or validation_type_key == 3 :
                    df_met_outcome_subset['DM_METRICS_OUTCOME_KEY_TGT'] = 0
                    # df_met_outcome_subset.loc[(df_met_outcome_subset['FIXED_VALUE_MIN'] == df_met_outcome_subset['METRIC_OUTCOME_VALUE']), 'STATUS_FLAG'] = 1
                    df_met_outcome_subset.loc[(df_met_outcome_subset['FIXED_VALUE_MIN'] != df_met_outcome_subset[
                        'METRIC_OUTCOME_VALUE']), 'STATUS_FLAG'] = -1
                elif validation_type_key == 2 or validation_type_key == 5:
                    df_met_outcome_subset['DM_METRICS_OUTCOME_KEY_TGT'] = 0
                    # df_met_outcome_subset.loc[(df_met_outcome_subset['METRICS_THRESHOLD'] >= df_met_outcome_subset['METRIC_OUTCOME_VALUE']), 'STATUS_FLAG'] = 1
                    df_met_outcome_subset.loc[(df_met_outcome_subset['FIXED_VALUE_MIN'] < df_met_outcome_subset[
                        'METRIC_OUTCOME_VALUE']), 'STATUS_FLAG'] = -1
                    df_met_outcome_subset.loc[(df_met_outcome_subset['FIXED_VALUE_MIN'] < df_met_outcome_subset[
                        'METRIC_OUTCOME_VALUE']), 'SEVERITY'] = 'Medium'
                    df_met_outcome_subset.loc[(df_met_outcome_subset['METRIC_OUTCOME_VALUE'] > df_met_outcome_subset[
                        'METRICS_THRESHOLD']), 'SEVERITY'] = 'High'
                    
                elif validation_type_key == 4:
                    df_met_outcome_subset_2 = df_met_outcome_subset_2.add_suffix('_TGT')
                    df_met_outcome_subset_2 = df_met_outcome_subset_2.rename(columns={"LVL_VALUE_1_TGT": "LVL_VALUE_1"})
                    df_met_outcome_subset = pd.merge(df_met_outcome_subset, df_met_outcome_subset_2, on=['LVL_VALUE_1'], how='inner')
                    df_met_outcome_subset.loc[(df_met_outcome_subset['METRIC_OUTCOME_VALUE'] != df_met_outcome_subset[
                        'METRIC_OUTCOME_VALUE_TGT']), 'STATUS_FLAG'] = -1

                # insert dataframe date into a temp table
                df_met_outcome_subset.to_sql("dm_comp_outcome_tmp", con=engine, index=False, if_exists="replace")

                # insert data into comp outcome table from temp table
                comp_outcome_insert_sql = " INSERT INTO DM_COMPARISON_OUTCOME(COMPARISON_KEY, RUN_DATE_INT, RUN_DATE, YEAR, MONTH, DAY, STATUS_FLAG, SOURCE_METRIC_OUTCOME_KEY, TARGET_METRIC_OUTCOME_KEY, SEVERITY)" \
                                          " SELECT COMPARISON_KEY, RUN_DATE_INT, RUN_DATE, YEAR, MONTH, DAY, STATUS_FLAG, DM_METRICS_OUTCOME_KEY, DM_METRICS_OUTCOME_KEY_TGT, SEVERITY FROM dm_comp_outcome_tmp"

                comp_outcome_insert_sql = comp_outcome_insert_sql + " WHERE NOT EXISTS" \
                                                                    "(SELECT COMPARISON_KEY FROM DM_COMPARISON_OUTCOME WHERE DM_COMPARISON_OUTCOME.COMPARISON_KEY = dm_comp_outcome_tmp.COMPARISON_KEY" \
                                                                    " AND DM_COMPARISON_OUTCOME.RUN_DATE_INT = dm_comp_outcome_tmp.RUN_DATE_INT)"

                conn.cursor().execute(comp_outcome_insert_sql)
                conn.commit()
            t1 = datetime.now()
            logger.info("Processed comparison key %s after %s" % (row['DM_COMPARISON_CONFIG_KEY'], (t1 - t0).total_seconds()))
            # df_comp_config.to_csv(r'd:\temp.csv',index = False, header=True)
            # DM_COMPARISON_CONFIG = pd.DataFrame()
            # drop temporary tables
            del df_met_outcome_subset
            del df_met_outcome_subset_2
            conn.cursor().execute("DROP TABLE IF EXISTS dm_metrics_outcome_tmp")
            conn.cursor().execute("DROP TABLE IF EXISTS dm_comp_outcome_tmp")

        # purge outcome table data
        conn.cursor().execute("DELETE FROM DM_METRICS_OUTCOME WHERE RUN_DATE < CURRENT_DATE() - 14")
        conn.cursor().execute("DELETE FROM DM_COMPARISON_OUTCOME WHERE RUN_DATE < CURRENT_DATE() - 14")
        logger.info("DM_METRICS_OUTCOME & DM_COMPARISON_OUTCOME table data has been purged.")


    except Exception as ex:
        logger.error("error in configuration outcome block %s: %s" %(str(ex), traceback.format_exc()))

    logger.info("Update comparison outcome - finished")


def get_cfg(config, path, default_value=None):
    tokens = path.split(".")
    cfg = config
    for t in tokens[0:-1]:
        cfg = cfg.get(t, {})
    leaf_prop = tokens[-1]
    if leaf_prop not in cfg and not default_value:
        raise Exception("Missing required %s configuration property. Check your YAML configuration file" % (leaf_prop))
    return cfg.get(leaf_prop, default_value)

def load_config(config_fn):
    # load yaml file according to YAML version
    yaml_version = yaml.__version__
    #config_file = open("./Config_DataValidaton.yaml", "r")
    config_file = open(config_fn, "r")

    if yaml_version > str(5.1):
        config_file = yaml.load(config_file, Loader=yaml.FullLoader)
    else:
        from yaml import load, dump
        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper
        config_file = load(config_file)

    return config_file

# call main
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''\
         additional information:
             You can run the script for below two filter conditions.
             "--filter_cond" which actually runs the routine for either specific business group or specific table. Accordingly pass the parameter value.
         ''')
    )

    parser.add_argument(
        "--config", "-c",
        help="Name of the configuration file"
    )

    parser.add_argument(
        "--filter_cond", "-f", action='store_true', default=True,
        help="Runs the benchmark"
    )

    args = parser.parse_args()

    if not args.config:
        print("Please indicate a configuration file, ex: MetricsValidationRoutine.py -c ./Config_DataValidaton.yaml")
       ## exit(0)

    config_file = args.config
    config = load_config(config_file)
    main(config, args.filter_cond)
