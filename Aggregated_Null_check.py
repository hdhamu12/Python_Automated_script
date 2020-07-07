#THis script will design a SQL statement to calculate a aggregated null check calculation taking a list of columns as an input dynamically

COLUMN_TO_VALIDATE='POWER_GEN_INSTCAP_EST,GAS_PRICES_EST,POWER_MARKET_EST,POWER_PRICES_DAYAHEAD_EST,POWER_GEN_REL_MW_100_EST_UOM,POWER_GEN_IGVP_EST,POWER_GEN_MW_EST'
PARAM_1 = 'year(row_timestamp) as year, month(row_timestamp) as month'
PARAM_2 = 'year, month'
COLUMNS_GROUP_BY_COND='year(row_timestamp), month(row_timestamp)'
DESTINATION_TBL='SVC_MKTCOND.PM_COMBINED_OP_HOURLY_MAT'
my_list = COLUMN_TO_VALIDATE.split(",")
j=1
space="'"
rec_count=""
main_select="select " + PARAM_2.replace(",","||';'||") +"||';'||"
column_value="select "+PARAM_1 + ","
for i in my_list:
     column_value = column_value + str(space)+ str(i) + str(space)+" as c" +str(j) + ", round(sum(case when "+i+" is null then 1 else 0 end)*100/count(*)) as value"+str(j)+","
     main_select=main_select + " c"+str(j) + "||';'||value" + str(j) +"||';'||"
     rec_count=rec_count+"value"+str(j)+"+"
     j+=1
     inner_query = column_value[:-1]+ " from " + DESTINATION_TBL +" group by "+ COLUMNS_GROUP_BY_COND +" order by " + COLUMNS_GROUP_BY_COND
metric_sql = main_select[:-7]+" AS GROUP_BY_VAL, ("+ rec_count[:-1] +")" + " as REC_COUNT from (" + inner_query + ");"
print(metric_sql)
