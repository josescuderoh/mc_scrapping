# Imports
from helper import *
import pandas as pd
import psycopg2
from collections import defaultdict
import numpy as np

# Function call to download and store raw static files
collect_static_files()

# Data required for month information. This months are mapped to a calendar one month behind
month_map = {'Enero':1,
              'Febrero':2,
              'Marzo':3,
              'Abril':4,
              'Mayo':5,
              'Junio':6,
              'Julio':7,
              'Agosto':8,
              'Septiembre':9,
              'Octubre':10,
              'Noviembre':11,
              'Diciembre':12}

month_sold_map = {'Enero':12,
              'Febrero':1,
              'Marzo':2,
              'Abril':3,
              'Mayo':4,
              'Junio':5,
              'Julio':6,
              'Agosto':7,
              'Septiembre':8,
              'Octubre':9,
              'Noviembre':10,
              'Diciembre':11}

########## Set filters ##########
# Include only following classes
classes = ['AUTOMOVIL','CAMIONETA', 'CAMIONETA PASAJ.','CAMPERO']
#Filter by service
id_service = 1
#Exclude following makes
manuf = ['ALEKO','AROCARPATI']

######### Read table and import to database #########

#Connect to database
try:
    conn = psycopg2.connect("dbname='matchcars' user='jescudero' host='localhost' password='654321+*'")
except:
    print("Unable to connect to the database")

#Read existing folders
folders_path = os.getcwd()+paths['files']
folders = sorted(os.listdir(path= folders_path))

#Iterate over folders
for folder in folders:
    #Temporary path
    temp_path = os.path.join(folders_path,folder)
    #Extract temporary information from folder
    folder_dic = {'reference': int(folder.split(sep="_")[0]),
                  'month_guide': month_map.get(folder.split(sep="_")[1]),
                  'month_sold': month_sold_map.get(folder.split(sep="_")[1]),
                  'year_guide': int(folder.split(sep="_")[2])}
    #List files in folder
    temp_files = os.listdir(temp_path)
    #Read required columns of code file (see new_sql)
    cols = [0,1,2,3,5,6,7,8,9,12,13,14,15,16,17,19,20,23,24,25]
    names = ['novedad_new', 'make_new', 'clase_new', 'id_fasecolda', 'ref1_new',
    'ref2_new','ref3_new','peso_new','IdServicio','importado_new','power_new','gearbox',
    'engine_cap', 'nacionalidad_new', 'pasajeros_new', 'doors', 'has_ac', 'fuel_system',
    'torque', 'um_new']
    #Read codes csv
    iter_codes = pd.read_csv(os.path.join(temp_path,temp_files[0]),
                              header=0,sep=",", usecols=cols, iterator=True, chunksize=1000,
                        dtype={'id_fasecolda':str, 'um_new':str, 'has_ac':str, 'importado_new': str},
                             names=names)
    codes = pd.concat([chunk[(chunk.IdServicio == id_service) &
                             (~chunk.make_new.isin(manuf)) &
                             (chunk.clase_new.isin(classes))] for chunk in iter_codes])
    #Select relevant codes
    codes = codes.drop(labels='IdServicio', axis=1)
    #Send to DB
    insertCars(conn,codes)

    #Read required registers of values files
    names = ['id_fasecolda', 'year_model', 'price']
    iter_prices = pd.read_csv(os.path.join(temp_path,temp_files[1]),
                               iterator=True, chunksize=1000, header=0,sep=",",
                           dtype={'id_fasecolda': 'str'}, names = names)
    #Check for month to select model
    if folder_dic['month_guide'] > 7:
        prices = pd.concat([chunk[(chunk.year_model > folder_dic['year_guide']) &
                              (chunk.id_fasecolda.isin(codes.id_fasecolda))] for chunk in iter_prices])
    else:
        prices= pd.concat([chunk[(chunk.year_model == folder_dic['year_guide']) &
                              (chunk.id_fasecolda.isin(codes.id_fasecolda))] for chunk in iter_prices])
    #Select relevant codes
    folder_dic['year_model'] = prices.year_model.values.tolist()[0]
    prices = prices.drop(labels='year_model', axis=1)
    prices['reference'] = folder_dic['reference']
    #Send to DB
    insertMonthlyPrices(conn, prices, folder_dic)

    #Report
    print("Guide %(reference)s for month %(month_guide)s of %(year_guide)s was included" % folder_dic)


#Create table of models vs montly prices
cursor = conn.cursor()
sql_start = """
select distinct year_model,month_sold from monthly_prices 
inner join guides on monthly_prices.id_guide = guides.id_guide
where guides.id_guide = (select min(id_guide) from guides);
"""
cursor.execute(sql_start)
start = cursor.fetchall()[0]
sql_end = """
select distinct year_model,month_sold from monthly_prices 
inner join guides on monthly_prices.id_guide = guides.id_guide
where guides.id_guide = (select max(id_guide) from guides);
"""
cursor.execute(sql_end)
end = cursor.fetchall()[0]

#Iterate over table and build pandas dataframe
year = 2008
months = [7,8,9,10,11,12,1,2,3,4,5,6]
dict_regs = defaultdict(dict)
flag = False

#Iterate over months
while True:
    for month in months:
        #Obtain specific registers from db
        temp_sql = """
        select id_fasecolda, price, monthly_prices.id_guide from monthly_prices 
        inner join guides on monthly_prices.id_guide = guides.id_guide 
        where year_model="""+str(year)+""" and month_sold="""+str(month)+""";"""
        cursor.execute(temp_sql)
        #Obtain data
        full_regs = cursor.fetchall()
        if full_regs != []:
            ids = [pair[0] for pair in full_regs]
            regs = [pair[1] for pair in full_regs]
            ref = np.unique([pair[2] for pair in full_regs])[0] if full_regs != [] else None
            # Rearrange into data frame
            full_regs_df = pd.DataFrame({'regs':regs}, index=ids)
            #Update nested dictionaries with values for year-month
            temp_key = str(ref)+ "-" +str(year) + "-" + str(month).zfill(2)
            for index, row in full_regs_df.iterrows():
                dict_regs[index][temp_key]= row.regs
            # Report
            print(temp_key+" updated")
            if month == end[1] and year == end[0]:
                flag = True
                break
    if flag==True:
        break
    #Update counter
    year += 1

monthly_prices_df =pd.DataFrame.from_dict(dict_regs,orient='index')
monthly_prices_df.sort_index(axis=1, inplace=True)

