# Imports
from bs4 import BeautifulSoup
import urllib
import os
import time
import zipfile
from selenium import webdriver
import json

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



def collect_static_files():
    """Function to download and extract raw pdf and csv files for new prices from
    the fasecolda website and store these static files into local directory"""

    # Root website
    root = "http://fasecolda.colserauto.com/fasecolda.explorador/"

    # Urls to download documents and files
    urls = {"docs": r"Default.aspx?url=E:\WWWROOT\FASECOLDA\Fasecolda.Web\Archivos\Guias\Documentos",
            "files": r"Default.aspx?url=E:\WWWROOT\FASECOLDA\Fasecolda.Web\Archivos\Guias\GuiaValores_NuevoFormato"}

    paths = {"docs": r"..\..\data\docs",
             "files": r"..\..\data\files"}

    # Extract fles for both urls
    if urls.keys():
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
                    file_codes = "GuiaCodigos_CSV_" + folder[0:3] + ".zip"
                    files_crawler.get_file(file_codes)
                    # Unzip the file
                    unzip(paths[key] + "\\" + folder + "\\" + file_codes,
                          paths[key] + "\\" + folder)
                    # Delete the zip from folder
                    os.remove(paths[key] + "\\" + folder + "\\" + file_codes)
                    # Get values csv file
                    file_values = "GuiaValores_CSV_" + folder[0:3] + ".zip"
                    files_crawler.get_file(file_values)
                    # Unzip the file
                    unzip(paths[key] + "\\" + folder + "\\" + file_values, paths[key] + "\\" + folder)
                    # Delet the zip from folder
                    os.remove(paths[key] + "\\" + folder + "\\" + file_values)
                    # Close browser
                    files_crawler.close_host()
