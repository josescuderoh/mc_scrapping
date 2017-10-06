# Imports
from helper import *
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from collections import defaultdict
import numpy as np

# Root website
root = "http://fasecolda.colserauto.com/fasecolda.explorador/"

# Urls to download documents and files
urls = {"docs": r"Default.aspx?url=E:\WWWROOT\FASECOLDA\Fasecolda.Web\Archivos\Guias\Documentos",
        "files": r"Default.aspx?url=E:\WWWROOT\FASECOLDA\Fasecolda.Web\Archivos\Guias\GuiaValores_NuevoFormato"}

paths = {"docs": r"..\..\data\docs",
         "files": r"..\..\data\files"}


# Extract fles for both urls
for key in urls.keys():
    # Paste url
    temp_url = root+urls[key]
    # Parse html
    if key == 'docs':
        # Read and parse html content
        temp_text = urllib.request.urlopen(temp_url).read()
        temp_soup = BeautifulSoup(temp_text,'html.parser')
        # Get all pdf file names from html structure
        files = [file.text for file in temp_soup.find_all('a') if file.text.endswith("pdf")]
        # Import existing files
        existing = os.listdir(os.path.join(os.getcwd(),paths[key]))
        # Check for missing files
        missing = sorted(set(files) - set(existing))
        # Update if there are missing files
        if missing:
            # Create crawler
            docs_crawler = Crawler()
            docs_crawler.create_profile(paths[key])
            # Open browser
            docs_crawler.open_host(temp_url)
            # Iterate over files and download document
            for file in missing:
                #Get current file
                docs_crawler.get_file(file)
            # Close browser
            docs_crawler.close_host()
    elif key == "files":
        # Extract and parse the html content
        temp_text = urllib.request.urlopen(temp_url).read()
        temp_soup = BeautifulSoup(temp_text,'html.parser')
        # Get all file names from html structure starting from the 135
        folders = [folder.text for folder in temp_soup.find_all('a') if folder.text >= '135'][1:]
        # Import existing files
        existing = os.listdir(os.path.join(os.getcwd(), paths[key]))
        # Check for missing files
        missing = sorted(list(set(folders) - set(existing)))
        # Iterate over missing folders
        for folder in missing:
            # Get the url of the folder
            folder_url = temp_url + "\\" + folder
            # Create crawler
            files_crawler = Crawler()
            files_crawler.create_profile(paths[key]+"\\"+folder)
            # Open browser
            files_crawler.open_host(folder_url)
            # Get codes csv file
            file_codes = "GuiaCodigos_CSV_"+folder[0:3]+".zip"
            files_crawler.get_file(file_codes)
            # Unzip the file
            unzip(paths[key]+"\\"+folder+"\\"+file_codes,
                  paths[key]+"\\"+folder)
            # Delete the zip file
            os.remove(paths[key]+"\\"+folder+"\\"+file_codes)
            # Get values csv file
            file_values = "GuiaValores_CSV_" + folder[0:3] + ".zip"
            files_crawler.get_file(file_values)
            # Unzip the file
            unzip(paths[key]+"\\"+folder+"\\"+file_values, paths[key]+"\\"+folder)
            # Remove the zip
            os.remove(paths[key] + "\\" + folder + "\\" + file_values)
            #Close browser
            files_crawler.close_host()


########## Data for including month ##########

#This months are mapped to a calendar one month behind
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

