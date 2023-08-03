import pandas as pd
pd.set_option('display.max_colwidth', 25)
pd.set_option('display.max_columns', 4)
pd.set_option('display.max_rows', 10)

df = pd.read_csv("C://Users//Z0045CEY//Downloads//MedicionEJ_202308_01 (2).csv", header = [0,1], skiprows=1)
a,b = df.columns.names

result = pd.melt(df, id_vars = [('Fecha','Hora')], var_name=['Company Name','Unit Name'])

result.replace('^\s+', '', regex=True, inplace=True) #front
result.replace('\s+$', '', regex=True, inplace=True) #end
result.rename(columns={result.columns[0]: 'Date'}, inplace=True)
#result['Unit Name'] = rtrim(result['Unit Name'] )
print(result.columns)
result = result.dropna(subset = ["value"])
