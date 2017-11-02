# Imports
from bs4 import BeautifulSoup
import urllib
import os
import time
import zipfile
from selenium import webdriver
import pandas as pd

def unzip(path_to_zip_file,directory_to_extract_to):
    """Unzips the files using an origin and destination directory"""
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(directory_to_extract_to)
    zip_ref.close()

def download_file(download_url):
    """Downloads a pdf document given a url"""
    response = urllib.request.urlopen(download_url)
    file = open("document.pdf", 'wb')
    file.write(response.read())
    file.close()
    print("Completed")

class Crawler:
    """This class creates a Crawler for the fasecolda website and downloads into a
    local path specific documents from the website"""
    def __init__(self):
        """Initialization method"""
        self.mime_types = "application/pdf,application/vnd.adobe.xfdf," \
                          "application/vnd.fdf,application/vnd.adobe.xdp+xml," \
                          "application/octet-stream"
        self.cur_dir = os.getcwd()

    def create_profile(self,download_path):
        """This method creates one profile for the browser in order to download the files"""
        # Create profile for firefox
        self.fp = webdriver.FirefoxProfile()
        self.download_path = os.path.join(self.cur_dir, download_path)
        # Select custom location
        self.fp.set_preference('browser.download.folderList', 2)
        # Do not show browser
        self.fp.set_preference('browser.download.manager.showWhenStarting', False)
        # Add path for download
        self.fp.set_preference('browser.download.dir', self.download_path)
        # Disable dialog box
        self.fp.set_preference("browser.helperApps.neverAsk.saveToDisk", self.mime_types)
        self.fp.set_preference("plugin.disable_full_page_plugin_for_types", self.mime_types)
        self.fp.set_preference("pdfjs.disabled", True)

    def open_host(self,host):
        """This method opens the broser given the previously defined driver and profile"""
        #Create instance of browser
        self.browser = webdriver.Firefox(firefox_profile=self.fp)
        self.browser.get(host)

    def get_file(self,file):
        """Downloads file by searching for a given link text and clicks it to download it"""
        print("Downloading "+ file)
        # Define element
        element = self.browser.find_element_by_link_text(file)
        # Actions
        element.location_once_scrolled_into_view
        element.click()
        # Wait until download is finished
        while os.stat(os.path.join(self.download_path,file)).st_size == 0:
            time.sleep(1)

    def close_host(self):
        """Closes browser"""
        self.browser.close()
        print("Browser closed")

def insertGuide(conn, dict_guide):
    """This method inserts guide information into the database"""
    #Create cursor
    temp_cursor = conn.cursor()
    #Create sql string
    sql_str = """INSERT INTO guides("year_guide", "month_guide", "month_sold", "created_at", "updated_at", "reference") 
                VALUES (%(year_guide)s, %(month_guide)s, %(month_sold)s, (select NOW()), (select NOW()), %(reference)s)
                on conflict (reference) do
                update set
                    updated_at = excluded.updated_at;"""
    #Send to database
    temp_cursor.execute(sql_str, dict_guide)
    conn.commit()
    print("Guide {} migration finished.".format(dict_guide['reference']))


def get_variations_by_make(conn):
    # Create cursor
    temp_cursor = conn.cursor()
    #Query database
    sql_str = """select distinct makes.name, max_price_percentage, min_price_percentage, med_price_percentage, good_price_percentage, max_level, min_level from price_variations
                 join yearly_prices on yearly_prices.id = price_variations.yearly_price_id
                 join cars on yearly_prices.car_id = cars.id 
                 join models on models.id = cars.model_id 
                 join makes on makes.id = models.make_id;"""
    temp_cursor.execute(sql_str)
    price_variations = temp_cursor.fetchall()
    #Create data frame
    variations_df = pd.DataFrame(price_variations,
                                       columns=["make", "max_price_percentage", "min_price_percentage",
                                                "med_price_percentage", "good_price_percentage", "max_level",
                                                "min_level"])
    return variations_df.set_index(['make'])


def insertPriceVariations(conn, d_frame, dict_guide):
    """Inserts prices into price_variations in order to update information about new prices."""
    # Create cursor
    temp_cursor = conn.cursor()
    #Get current variations by make
    variations_by_make = get_variations_by_make(conn)
    # Obtain existing cars in database
    sql_str = """select id,id_fasecolda from cars where id_fasecolda in {}"""
    temp_cursor.execute(sql_str.format(tuple(d_frame.index)))
    valid_tuples = [tup_id for tup_id in temp_cursor.fetchall()]
    # Create list of dictionaries
    yearly_prices  = []
    for id,id_fasecolda in valid_tuples:
        yearly_prices.append({'car_id': id, 'year_model': d_frame.loc[id_fasecolda].model_year.item()})
    #Insert yearly prices
    sql_str = """insert into yearly_prices (car_id, year_model, created_at, updated_at)
                 select %(car_id)s, %(year_model)s, (select NOW()), (select NOW())
                 where not exists 
                 (select id from yearly_prices where car_id = %(car_id)s and year_model=%(year_model)s);"""
    # Send to yearly_prices
    temp_cursor.executemany(sql_str, yearly_prices)
    # Count changes and commit
    new_yearly_prices_count = temp_cursor.rowcount
    conn.commit()

    # Create list of dictionaries with required information
    dict_list = []
    for car_id, id_fasecolda in valid_tuples:
        temp_d_frame = d_frame.loc[id_fasecolda]
        temp_variations = variations_by_make.loc[d_frame.loc[id_fasecolda].make]
        temp_dict ={
            'car_id': car_id,
            'year_model': temp_d_frame.model_year.item(),
            'market_price': temp_d_frame.price.item(),
            'max_price_percentage': temp_variations.max_price_percentage.item(),
            'min_price_percentage': temp_variations.min_price_percentage.item(),
            'med_price_percentage': temp_variations.med_price_percentage.item(),
            'good_price_percentage': temp_variations.good_price_percentage.item(),
            'max_level': temp_variations.max_level.item(),
            'min_level': temp_variations.min_level.item(),
            'reference': dict_guide['reference'],
        }
        dict_list.append(temp_dict)
    # Store yearly_prices ids in database
    sql_str = """insert into price_variations (yearly_price_id, market_price, max_price_percentage, min_price_percentage, 
                med_price_percentage, good_price_percentage, max_level, min_level, created_at, updated_at, id_guide_pk)
                select (select id from yearly_prices where car_id=%(car_id)s and year_model=%(year_model)s), %(market_price)s, 
                %(max_price_percentage)s, %(min_price_percentage)s, %(med_price_percentage)s, %(good_price_percentage)s, 
                %(max_level)s, %(min_level)s, (select now()), (select now()), 
                (select id from guides where reference=%(reference)s)
                where not exists
                (select id from price_variations where 
                yearly_price_id = (select id from yearly_prices where car_id=%(car_id)s and year_model=%(year_model)s) 
                and id_guide_pk=(select id from guides where reference=%(reference)s));"""
    temp_cursor.executemany(sql_str, dict_list)
    # Count changes and commit
    new_prices_count = temp_cursor.rowcount
    conn.commit()
    print("{} new yearly prices and {} new price variations of {} registers sent.".format(new_yearly_prices_count,
                                                                                          new_prices_count,
                                                                                          len(dict_list)))


def collect_static_files(root, urls, paths):
    """Function to download and extract raw pdf and csv files for new prices from
    the fasecolda website and store these static files into local directory"""

    # Extract fles for both urls
    try:
        for key in urls.keys():
            # Paste url
            temp_url = root + urls[key]
            # Parse html
            if key == 'docs':
                # Read and parse html content
                temp_text = urllib.request.urlopen(temp_url).read()
                temp_soup = BeautifulSoup(temp_text, 'html.parser')
                # Get all pdf file names from html structure
                files = [file.text for file in temp_soup.find_all('a') if file.text.endswith("pdf")]
                # Import existing files
                existing = os.listdir(os.path.join(os.getcwd(), paths[key]))
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
                        # Get current file
                        docs_crawler.get_file(file)
                    # Close browser
                    docs_crawler.close_host()
            elif key == "files":
                # Extract and parse the html content
                temp_text = urllib.request.urlopen(temp_url).read()
                temp_soup = BeautifulSoup(temp_text, 'html.parser')
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
                    files_crawler.create_profile(paths[key] + "\\" + folder)
                    # Open browser
                    files_crawler.open_host(folder_url)
                    # Get codes csv file
                    file_codes = "GuiaCodigos_TxtPipe_" + folder[0:3] + ".zip"
                    files_crawler.get_file(file_codes)
                    # Unzip the file
                    unzip(paths[key] + "\\" + folder + "\\" + file_codes,
                          paths[key] + "\\" + folder)
                    # Delete the zip from folder
                    os.remove(paths[key] + "\\" + folder + "\\" + file_codes)
                    # Get values csv file
                    file_values = "GuiaValores_TxtPipe_" + folder[0:3] + ".zip"
                    files_crawler.get_file(file_values)
                    # Unzip the file
                    unzip(paths[key] + "\\" + folder + "\\" + file_values, paths[key] + "\\" + folder)
                    # Delet the zip from folder
                    os.remove(paths[key] + "\\" + folder + "\\" + file_values)
                    # Close browser
                    files_crawler.close_host()
        print("Databases are up to date now.")
    except:
        return("An error was encountered.")


def select_models(folder_dic, codes, file_path):
    #Field names
    names = ['id_fasecolda', 'model_year', 'price']
    # Read all ids from file
    all_prices = pd.read_table(file_path, header=0,sep="|", index_col=False,
                           dtype={'id_fasecolda': 'str'}, names = names)
    all_prices = all_prices.set_index(['id_fasecolda'])
    # Get latest model_year
    max_model = max(all_prices.model_year)
    #Add right new prices to codes
    codes = codes.set_index(['id_fasecolda'])
    codes['price'] = 0
    codes['model_year'] = 0

    #Set values only if codes are last model
    codes.model_year = max_model
    selected_um = codes.index[codes.um].values
    for id in selected_um:
        codes.set_value(id, 'price', all_prices.price[(all_prices.index == id) & (all_prices.model_year == max_model)])

    if max_model != folder_dic['year_guide']:
        #Build temporary dataframe with two last years
        temp_df = pd.concat({'price_min': all_prices.price[(all_prices.model_year == folder_dic['year_guide'])],
                             'price_max': all_prices.price[(all_prices.model_year == max_model)]}, axis=1, join='inner')
        #Set values if codes are not last model only when prices in both years are the same
        selected_nonum = [code for code in temp_df.index[temp_df.price_min == temp_df.price_max].values if code in codes.index]
        #Iterate and add prices
        for id in selected_nonum:
            codes.set_value(id, 'price', all_prices.price[(all_prices.index == id) & (all_prices.model_year == max_model)])
            codes.set_value(id, 'model_year', folder_dic['year_guide'])

    #Return df
    prices_df = codes.loc[:,['make', 'model_year','price']][codes['price']>0]
    return prices_df

