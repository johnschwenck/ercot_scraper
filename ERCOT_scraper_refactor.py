#!/usr/bin/env python
# coding: utf-8

# ## ERCOT Web Scrape

import sys
import os
from bs4 import BeautifulSoup
import requests
import zipfile, io
from datetime import datetime
import re
from threading import Thread
import timeit
import time


# Holy Grail of ERCOT Web Scrape, main function, call function with or without an intended path, if no path is given path defaults to script's folder
# If no path exists in folder script will generate it
def data_extract(user_path=None): 
    '''Downloads desired datatypes from ERCOT MIS. 
    If no path is given, function will check if a path exists in this file's folder and created the needed files if they do not exist.
    Will require user input to select which datatypes need to be downloaded.
    
    Args: 
        user_path: string with the file path where the user wants to put the downloaded files, can be empty
        
    Returns:
        Will download files either to the desired path or to the folder where this file is located.
    '''
    # If path is not given, run function for user to setup path
    start1 = timeit.default_timer()
    # This will either find an existing path or generate a new path
    if user_path is None:
        setup = user_setup()
        ERCOT_path = setup[1]
        DataDict = setup[0]
    else:
        # Check that there actually is a directory with necessary folders
        # TODO: make this directory check its own function which returns a boolean if it completed correctly or not
        if os.path.isdir(str(user_path)+'\\ERCOT Data') == False:
            # If there is not, have the user select if they wish to create new folders in the given directory
            newsetup = input('No folders found! \nRedirecting to user setup...\n \nWould you like to create the necessary folders in current directory ('+os.path.join(user_path,'')+') or would you like to choose another location and run setup assistant?\n  1) Create folders in '+user_path+' \n  2) Choose alternative location\n  3) Exit')
            
            # Runs function to create new folders
            if newsetup is str(1):
                tmp_path = dir_create(user_path)
                setup = user_setup(tmp_path)
                DataDict = setup[0]
                ERCOT_path = setup[1]
            
            # Runs function to choose new path
            elif newsetup is str(2):
                setup = user_setup()
                DataDict = setup[0]
                ERCOT_path = setup[1]
            
            # Exits session
            elif newsetup is str(3):
                sys.exit('Leaving current state... \n\nSession aborted.')
            
            # Error message and exit
            else:
                sys.exit('ERROR: Could not complete setup. Please try again.')
        # If there is a directory with the necessary folders, run the rest of the setup functions
        else:
            ERCOT_path = user_path
            DataDict = DataDict_setup(ERCOT_path)
            
    # Run function for user to select data sets
    user_selections = source_selection()
    
    # For loop to loop through user's requested data sets and to find the download links and download all files
    for selection in user_selections:
        for count, value in enumerate(DataDict[selection]):
            # Define what data set is being downloaded and the links for it
            start = timeit.default_timer()
            data_name = DataDict[selection][count][0]
            ERCOT_link = DataDict[selection][count][1]
            filepath = DataDict[selection][count][2]
            
            # Runs the function to get the download links of the required data set
            download_links = scrape_for_download(ERCOT_link)
            # Downloads the files of the required data set
            download_files_from_array(download_links, filepath)
            
            # Confirm that the dataset has been downloaded
            stop = timeit.default_timer()
            print('Downloaded files for ' + selection + " " + data_name + ' in ' + str(round((stop-start), 2)) + ' seconds')
            # Artificial 5 second wait to keep firewall happy, we don't need speed anyways 
            time.sleep(5)
            
    # When all datasets are downloaded, print confirmation and return to given/found/created path
    print('Job complete')
    start2 = timeit.default_timer()
    print('Took ' + str(round((start2-start1), 2)) + ' seconds')
    os.chdir(ERCOT_path)
        


# Creates a dictionary of the different links to each dataset from ERCOT MIS:
def DataDict_setup(ERCOT_path):
    '''Creates the dictionary of different datasets and links to those datasets, must provide path argument
    
    Args: 
        ERCOT_path: string with the file path where the user wants to put the downloaded files
        
    Returns:
        DataDict: dictionary of the different datatypes, which includes their name, link, and desired download path
    '''
    
    DataDict = {'DAM':[ ['Total Energy Purchased',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12333&reportTitle=DAM%20Total%20Energy%20Purchased&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/Energy Purchased & Sold/Purchased'],
                        ['Total Energy Sold',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12334&reportTitle=DAM%20Total%20Energy%20Sold&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/Energy Purchased & Sold/Sold'],
                        ['DAM SPPs',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12331&reportTitle=DAM%20Settlement%20Point%20Prices&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/SPP'],
                        ['Shadow Prices',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12332&reportTitle=DAM%20Shadow%20Prices&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/Shadow Prices'],
                        ['PTP Obligations',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13042&reportTitle=DAM%20PTP%20Obligation%20and%20Option%20Results%20by%20Settlement%20Point&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/PTP Obligations'],
                        ['PTP Option Prices',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=10042&reportTitle=Day-Ahead%20Point-to-Point%20Option%20Price%20Report&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/PTP Option Prices'],
                        ['Historical SPPs (Hourly) by Load Zone and Hub',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13060&reportTitle=Historical%20DAM%20Load%20Zone%20and%20Hub%20Prices&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/Historical SPP - LZ & Hub']],

                'RTM':[['RTM SPPs',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12301&reportTitle=Settlement%20Point%20Prices%20at%20Resource%20Nodes,%20Hubs%20and%20Load%20Zones&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/RTM/SPP'],
                                ['SCED Shadow Prices',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12302&reportTitle=SCED%20Shadow%20Prices%20and%20Binding%20Transmission%20Constraints&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/RTM/SCED Shadow Prices'],
                                ['Historical SPPs (15 min) by Load Zone and Hub',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13061&reportTitle=Historical%20RTM%20Load%20Zone%20and%20Hub%20Prices&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/RTM/Historical SPP - LZ & Hub']],

                'LMP':[['DAM Hourly LMPs',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12328&reportTitle=DAM%20Hourly%20LMPs&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/DAM/LMP - Hourly DAM'],
                                ['RTM Resource Node LMPs (5 min)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12300&reportTitle=LMPs%20by%20Resource%20Nodes,%20Load%20Zones%20and%20Trading%20Hubs&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/RTM/LMP - Resource Node - Load Zone - Hub'],
                                ['RTM Electric Bus LMPs (5 min)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=11485&reportTitle=LMPs%20by%20Electrical%20Bus&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Market Information/RTM/LMP - Electrical Bus']],

                'Load':[['Real Time Load - Actual vs Forecast (Hourly) (System Wide)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13499&reportTitle=Hourly%20Real-Time%20Load%20vs.%20Actual%20Report&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Load/Actual/Real Time Load - Actual vs Forecast (Hourly)'],
                                ['Actual System Load (Hourly by Weather Zone)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13101&reportTitle=Actual%20System%20Load%20by%20Weather%20Zone&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Load/Actual/Actual System Load - Weather Zone'], # <-- make sure to remove the "]"
                                ['Intrahour Load Forecast (by Weather Zone)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=16553&reportTitle=Intra-Hour%20Load%20Forecast%20by.%20Weather%20Zonet&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Load/Actual/IntraHour Forecast - Weather Zone'],
                                ['Seven Day Load Forecast (Hourly by Weather Zone)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12312&reportTitle=Seven-Day%20Load%20Forecast%20by%20Weather%20Zone&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Load/Actual/Seven-Day Load Forecast - Weather Zone']],

                'Generation':[['System Wide & LZ Wind Power Production: Actual vs Forecast (Hourly)',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13028&reportTitle=Wind%20Power%20Production%20-%20Hourly%20Averaged%20Actual%20and%20Forecasted%20Values&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Generation/Wind/System Wide & LZ Wind Production - Act vs Fcast'],
                                ['IntraHour Wind Production Actual - Load Zone',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=14788&reportTitle=Wind%20Power%20Production%20-%20Actual%205-Minute%20Actual%20and%20Averaged%20Values%20by%20Geographical%20Region&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Generation/Wind/IntraHour - Actual'], # <-- make sure to remove the "]" and add a ","
                                ['IntraHour Wind Production Forecast - Load Zone',"http://mis.ercot.com/misapp/GetReports.do?reportTypeId=16554&reportTitle=Intra-Hour%20Wind%20Power%20Forecast%20By%20Geographical%20Region&showHTMLView=&mimicKey",ERCOT_path + '/ERCOT Data/Generation/Wind/IntraHour - Forecast']]
            }
    
    print("Dictionary of links created")
    return (DataDict)



# Create necessary folders within specified path:
def dir_create(user_path):
    '''General function to take a path and create the necessary folders for the ERCOT Data
            
    Args: 
        user_path: string with the file path where the user wants to put the downloaded files
        
    Returns:
        user_path
    '''
    
    if os.path.splitdrive(user_path)[1] == '\\':
        user_path = os.path.splitdrive(user_path)[0] 
    else:
        user_path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 

    os.mkdir(str(user_path)+'\\ERCOT Data')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information')
    
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold\\Purchased')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold\\Sold')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\LMP - Hourly DAM')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\PTP Obligations')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\PTP Option Prices')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Shadow Prices')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\SPP')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM\\Historical SPP - LZ & Hub')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM\\LMP - Electrical Bus')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM\\LMP - Resource Node - Load Zone - Hub')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM\\SPP')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM\\SCED Shadow Prices')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Fuel Mix')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind\\Historical Hourly Wind Output')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind\\IntraHour - Actual')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind\\IntraHour - Forecast')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind\\System Wide & LZ Wind Production - Act vs Fcast')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Generation\\Wind\\Wind Integration Reports')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Load')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Load\\Actual System Load - Weather Zone')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Load\\IntraHour Forecast - Weather Zone')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Load\\Real Time Load - Actual vs Forecast (Hourly)')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Load\\Seven-Day Load Forecast - Weather Zone')

    return user_path



# Pick from a list of suggested path locations for setting up necessary folders
# TODO: comment and make style/best practices edits
def user_setup(user_path = None):
    '''Use this function to have the user specify the desired path to create ERCOT Data folders
    
    Args: 
        user_path: string with the file path where the user wants to put the downloaded files, is allowed to be empty
    
    Returns:
        DataDict: dictionary of the different datatypes, which includes their name, link, and desired download path
        
        ERCOT_path: string with the file path where the user wants to put the downloaded files, is allowed to be empty
    '''

    if user_path is None:
        user_path = input("Choose path for creating necessary folders: \n  1)  (Current Directory) "+os.getcwd()+" \n  2)  (External Hard Drive) E:\ \n  3)  (External Hard Drive) F:\ \n  4)  Other \n  5)  Exit \n").upper()
        
        if user_path == str(5):
                sys.exit("Leaving current state... \n\nSession aborted.")
        elif user_path == str(1):
                user_path = os.path.join(os.getcwd(),'')
                if os.path.isdir(str(user_path)+'\\ERCOT Data') == False:
                    try:
                        yn = input('No folders found! Need to create necessary directory... \n   Proceed? y/n \n').lower()
                        if yn == 'y':
                            dir_create(user_path)
                        else:
                            sys.exit("Leaving current state... \n\nSession aborted.")
                    except ValueError:
                        print('ERROR: unrecognized character')
                else:
                    print('Folders already exist!')
                print('\nERCOT_path: \n' + user_path+"\n")
        elif user_path == str(2):
                user_path = 'E:\\'
                if os.path.isdir(str(user_path)+'\\ERCOT Data') == False:
                    try:
                        yn = input('No folders found! Need to create necessary directory... \n   Proceed? y/n \n').lower()
                        if yn == 'y':
                            dir_create(user_path)
                        else:
                            sys.exit("Leaving current state... \n\nSession aborted.")
                    except ValueError:
                        print('ERROR: unrecognized character')
                else:
                    print('Folders already exist!')
                print('\nERCOT_path: \n' + user_path+"\n")
        elif user_path == str(3):
                user_path = 'F:\\'
                if os.path.isdir(str(user_path)+'\\ERCOT Data') == False:
                    try:
                        yn = input('No folders found! Need to create necessary directory... \n   Proceed? y/n \n').lower()
                        if yn == 'y':
                            dir_create(user_path)
                        else:
                            sys.exit("Leaving current state... \n\nSession aborted.")
                    except ValueError:
                        print('ERROR: unrecognized character')
                else:
                    print('Folders already exist!')
                print('\nERCOT_path: \n' + user_path+"\n")
        elif user_path == str(4):
                user_path = input('Location to file path: \n')
                user_path = os.path.split(user_path)[0] + os.path.split(user_path)[1]
                if os.path.isdir(str(user_path)+'\\ERCOT Data') == False:
                    try:
                        yn = input('No folders found! Need to create necessary directory... \n   Proceed? y/n \n').lower()
                        if yn == 'y':
                            dir_create(user_path)
                        else:
                            sys.exit("Leaving current state... \n\nSession aborted.")
                    except ValueError:
                        print('ERROR: unrecognized character')
                else:
                    print('Folders already exist!')
                print('\nERCOT Data folders created in:\n'+user_path+"\n")
        else:
            sys.exit('ERROR: Could not find path')

    else:
        if os.path.splitdrive(user_path)[1] == '\\': # determine whether path is the actual drive (i.e. C:\\)
            user_path = os.path.splitdrive(user_path)[0]#+'\\'
        #elif os.path.split(user_path)[1] != '':
        #    path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 
        else:
            user_path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 
    
    ERCOT_path = user_path
    ERCOT_path = os.path.split(ERCOT_path)[0] + os.path.split(ERCOT_path)[1]
    DataDict = DataDict_setup(ERCOT_path)

    return (DataDict,ERCOT_path)


# Allows the user to select what they want to download from the given list
# TODO: comment and make style/best practices edits
def source_selection():
    '''Allows the user to select what datatypes they would want to download
    
    Returns:
        userselection: a list of what datatypes the user wants to download
    '''
    
    # Use this function to eliminate duplicate entries such as 112 or 1233 etc
    def uniqueCharacters(str): 
        # If at any time we encounter 2 same characters, return false 
        for i in range(len(str)): 
            for j in range(i + 1,len(str)):  
                if(str[i] == str[j]): 
                    return False
        # If no duplicate characters encountered, return true 
        return True

    source_options = {
        '1':'DAM',
        '2':'RTM',
        '3':'LMP',
        '4':'Load',
        '5':'Generation',
        '6':'All',
        '7':'Exit'}

    omit = set('890')
    while True:
        try:
            sourcetype = input('\nWhich source type would you like to combine?\nSelect all that apply:\n\n  1) DAM\n  2) RTM\n  3) LMP\n  4) Load\n  5) Generation\n  6) All\n  7) Exit\n')
            if (sourcetype.count('6') == 0 and sourcetype.count('7') == 0 and uniqueCharacters(sourcetype) and int(sourcetype) >= 1 and int(sourcetype) <= 55555 and any(n in sourcetype for n in omit) == False): 
                break
            elif sourcetype == '6' or sourcetype == '7':
                break
            else:
                print('ERROR: Incorrect entry. Please use only numeric values from 1 - 7 according to the above options')
                continue
        except ValueError:
            print('ERROR: Incorrect entry. Please use only numeric values from 1 - 7 according to the above options')
            continue

    if sourcetype == str(7):
        sys.exit('Leaving Session... \n\nSession aborted.')
    elif sourcetype == str(6):
        sourcetype = ['DAM','RTM','LMP','Load','Generation']
    elif sourcetype != str(6):
        source_selection = []
        for i in sourcetype:
            source_selection.append(source_options[i])
            sourcetype = source_selection
    userselection = sourcetype
    return(userselection)



# This function scrapes the given ERCOT MIS table for the download links
def scrape_for_download(ERCOT_link):
    '''Scrapes the given webpage for all download links that are not xml, returns an array of download links to iterate through
    
    Args:
        ERCOT_link: a string containing the link of the MIS directory for this datatype
        
    Returns:
        download_links: an array containing all the direct download links for all downloadable files in the MIS directory for that datatype
    '''
    
    # bs4 stuff to read the html
    requests_response  = requests.get(ERCOT_link)
    htmldata = requests_response.text
    htmlpage = BeautifulSoup(htmldata, features="lxml")
    
    # Generate the empty array for the download links
    download_links = []
    
    # First for loop loops through the first layer of of table data. At first glance this does not make sense, but for some reason this is needed otherwise it will create duplicates
    for table in htmlpage.find_all('tr'):
        # Second for loop loops through the second layer of the table data, this is the row data.
        for row in table.find_all('tr'):
            # If the row does not include 'xml' and includes 'href', we want to find the hyperlink and save the link to the array
            if 'xml' not in str(row) and 'href' in str(row):
                current_link = row.find('a').get('href')
                download_links.append('http://mis.ercot.com' + current_link)
                
    # Return the array of download links            
    return download_links



# This function starts the threads to download any files from the links from the array of download links given, and leave a text file listing urls and items downloaded
def download_files_from_array(download_links, filepath):
    '''Creates the threads to download files from the array of links given, 
    and each thread then calls download_file_from_link to download the file
    
    Args:
        download_links: an array containing all the direct download links for all downloadable files in the MIS directory for that datatype
            
        filepath: string with the file path where the user wants to put the downloaded files
    '''
    
    # Generate the output file name
    current_time = datetime.now().strftime("%H%M%S")
    outputfilename = 'output' + str(current_time) + '.txt'
    numberoffiles = 0
    
    # Download in batches of 100 so I don't get caught by CDN
    batchsize = 100
    for batch in range(0, len(download_links), batchsize):
        # Load up the different threads for this download, dramatically speeds up the download
        threads = []
        download_links_batch = download_links[batch:batch+batchsize]
        # First for loop creates a thread for each link to download it
        for link in download_links_batch:
            threads.append(Thread(target=download_file_from_link, args=(link,filepath,)))
        # Second for loop starts all the different threads
        for total in threads:
            total.start()
        # Third for loop waits for all the threads to finish
        for total in threads:
            total.join()
    
    # Use 'with open' to automatically close the file when we're done with the for loops 
    with open(filepath + '/' + outputfilename,'w') as outputfile:
        for link in download_links:
            # In each for loop iteration, write the url of the file downloaded
            outputfile.writelines(link + '\n')
            numberoffiles = numberoffiles + 1
    
        # Output last line in text file to record how many files were downloaded
        outputfile.writelines('Downloaded ' + str(numberoffiles) + ' files')



# This function downloads files from the given link
def download_file_from_link(link, filepath):
    '''Downloads files from the given link
    
    Args:
        link: a direct download link
        
        filepath: string with the file path where the user wants to put the downloaded files
        
    Raises:
        404: can raise a 404 error with the download link if the server refuses connection twice, or if connection fails twice
        
        FileExistsError: [WinError 183]: sometimes the check if the zipped file already exists fails
            I do not think this is a worry
    '''
        
    # Get the html data from the link, if the first try fails (most probably because it was refused by the server), wait 10 seconds and try again
    # I know doing a non-specific 'except' is bad, but any exceptions raised would be because of a connection error
    try:
        download_url = requests.get(link)
    except:
        time.sleep(10)
        download_url = requests.get(link)
    
    # Get the header information of the link, if getting the header fails, reacquire the html data from the link and try again
    # Should only need to retry once
    # I know doing a non-specific 'except' is bad, but any exceptions raised would be because of a connection error
    try:
        file_info = download_url.headers['content-disposition']
    except:
        download_url = requests.get(link)
        file_info = download_url.headers['content-disposition']

    # Regex the file information to get the name
    filename = re.findall("filename=(.+)", file_info)[0]
            
    # Check to see if file is a zip, if not just import it
    if '.zip' in filename:
        # If the file is a zip, edit the name to check if the underlying file already exists
        filename = filename[:-4]
        filename = filename.replace('_', '.')
    
        # If the file does not already exist in the folder, download the zip and extract it to the folder
        if not(os.path.isfile(filepath + '/' + filename)):
            # Use the io.BytesIO so I do not have to download the zip file to disk before I extract it
            zip = zipfile.ZipFile(io.BytesIO(download_url.content))
            zip.extractall(filepath)
        else:
            with(open(filepath + '/' + filename)) as filewrite:
                filewrite.write(download_url.content)