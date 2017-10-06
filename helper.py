import urllib
import os
import time
import zipfile
from selenium import webdriver
import json

def unzip(path_to_zip_file,directory_to_extract_to):
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(directory_to_extract_to)
    zip_ref.close()

def download_file(download_url):
    response = urllib.request.urlopen(download_url)
    file = open("document.pdf", 'wb')
    file.write(response.read())
    file.close()
    print("Completed")

class Crawler:
    """This class creates a Crawler for the fasecolda website and downloads into a local path specific documents from the website"""
    def __init__(self):
        """Initialization method"""
        self.mime_types = "application/pdf,application/vnd.adobe.xfdf,application/vnd.fdf,application/vnd.adobe.xdp+xml,application/octet-stream"
        self.cur_dir = os.getcwd()

    def create_profile(self,download_path):
        """This method creates one profile for the browser in order to download the files"""
        #Create profile for firefox
        self.fp = webdriver.FirefoxProfile()
        self.download_path = self.cur_dir + download_path
        #Select custom location
        self.fp.set_preference('browser.download.folderList', 2)
        #Do not show browser
        self.fp.set_preference('browser.download.manager.showWhenStarting', False)
        #Add path for download
        self.fp.set_preference('browser.download.dir', self.download_path)
        #Disable dialog box
        self.fp.set_preference("browser.helperApps.neverAsk.saveToDisk", self.mime_types)
        self.fp.set_preference("plugin.disable_full_page_plugin_for_types", self.mime_types)
        self.fp.set_preference("pdfjs.disabled", True)

    def open_host(self,host):
        """This method opens the broser given the previously defined driver and profile"""
        #Create instance of browser
        self.browser = webdriver.Firefox(firefox_profile=self.fp)
        self.browser.get(host)

    def get_file(self,file):
        """This method downloads file by searching for a given link text and clicks it to download it"""
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

def insertCars(conn, d_frame):
    #Create cursor
    temp_cursor = conn.cursor()
    #Create sql string
    sql_str = """INSERT INTO cars("novedad_new", "make_new", "clase_new", "id_fasecolda", "ref1_new", "ref2_new", "ref3_new", "peso_new", "importado_new", "power_new", "gearbox", "engine_cap", "nacionalidad_new", "pasajeros_new", "doors", "has_ac", "fuel_system", "torque", "um_new", "added_new") 
    VALUES (%(novedad_new)s, %(make_new)s, %(clase_new)s, %(id_fasecolda)s, %(ref1_new)s,%(ref2_new)s, %(ref3_new)s, %(peso_new)s, %(importado_new)s, %(power_new)s, %(gearbox)s, %(engine_cap)s, %(nacionalidad_new)s, %(pasajeros_new)s, %(doors)s, %(has_ac)s, %(fuel_system)s, %(torque)s, %(um_new)s, (select NOW()))
    on conflict (id_fasecolda) do 
    update set ("novedad_new", "make_new", "clase_new", "ref1_new", "ref2_new", "ref3_new", "peso_new", "importado_new", "power_new", "gearbox", "engine_cap", "nacionalidad_new", "pasajeros_new", "doors", "has_ac", "fuel_system", "torque", "um_new", "added_new") = (
    excluded.novedad_new, excluded.make_new, excluded.clase_new, excluded.ref1_new, excluded.ref2_new, excluded.ref3_new, excluded.peso_new, excluded.importado_new, excluded.power_new, excluded.gearbox, excluded.engine_cap, excluded.nacionalidad_new, excluded.pasajeros_new, excluded.doors, excluded.has_ac, excluded.fuel_system, excluded.torque, excluded.um_new, excluded.added_new);"""
    #Create list of dictionaries
    dict_list = []
    for i in range(len(d_frame)):
        dict_list.append(json.loads(d_frame.iloc[i].to_json()))
    #Send to database
    temp_cursor.executemany(sql_str, dict_list)
    conn.commit()

    return "Cars migration finished..."

def insertMonthlyPrices(conn, d_frame, folder_guide):
    #Create cursor
    temp_cursor = conn.cursor()
    #Insert guide
    sql_guides = """
    INSERT INTO guides("id_guide","year_guide","month_guide","year_model","month_sold",date_added)
    VALUES (%(reference)s, %(year_guide)s, %(month_guide)s, %(year_model)s, %(month_sold)s, (select NOW()));"""
    #Send to database
    temp_cursor.execute(sql_guides, folder_guide)
    conn.commit()

    #Insert prices
    sql_str = """
    INSERT INTO monthly_prices("id_fasecolda", "price", "id_guide", "date_added")
    VALUES (%(id_fasecolda)s, %(price)s, %(reference)s, (select NOW()))
    on conflict do nothing;"""
    #Create list of dictionaries
    dict_list = []
    for i in range(len(d_frame)):
        dict_list.append(json.loads(d_frame.iloc[i].to_json()))
    #Send to database
    temp_cursor.executemany(sql_str, dict_list)
    conn.commit()

    return "Prices migration finished..."

