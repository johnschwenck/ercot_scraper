#!/usr/bin/env python
# coding: utf-8

# ## ERCOT Web Scrape

# In[151]:


import sys
import os
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urllib import request
import requests
import zipfile, io
from pathlib import Path
from datetime import datetime
import glob



def DataDict_setup(ERCOT_path):
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
    return (DataDict)



# Create necessary folders within specified path:
def dir_create(user_path):
    '''General function to take a path and create the necessary folders for the ERCOT Data
            Must specify path argument'''
    
    if os.path.splitdrive(user_path)[1] == '\\':
        user_path = os.path.splitdrive(user_path)[0] 
    #elif os.path.split(user_path)[1] != '':
    #    path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 
    else:
        user_path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 
    
    #user_path = user_path
    #user_path = path

    os.mkdir(str(user_path)+'\\ERCOT Data')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\RTM')

    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold\\Purchased')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Energy Purchased & Sold\\Sold')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\LMP - Hourly DAM')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\PTP Obligations')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\PTP Option Prices')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\Shadow Prices')
    os.mkdir(str(user_path)+'\\ERCOT Data\\Market Information\\DAM\\SPP')

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
def user_setup(user_path = None):
    '''Use this function to have the user specify the desired path to create ERCOT Data folders'''

    if user_path is None:
        user_path = input("Choose path for creating necessary folders: \n  1)  (Current Directory) "+os.getcwd()+" \n  2)  (External Hard Drive) E:\ \n  3)  (External Hard Drive) F:\ \n  4)  Other \n  5)  Exit \n").upper()
        
        if user_path == str(5):
                sys.exit("Leaving current state... \n\nSession aborted.")
#############################################################################################################################
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
#############################################################################################################################
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
#############################################################################################################################
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
#############################################################################################################################
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
#############################################################################################################################
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
    #ERCOT_path = os.path.split(ERCOT_path)[0] + os.path.split(ERCOT_path)[1]+'/'
    #DataDict = DataDict_setup(ERCOT_path)
    ERCOT_path = os.path.split(ERCOT_path)[0] + os.path.split(ERCOT_path)[1]
    DataDict = DataDict_setup(ERCOT_path)

    return (DataDict,ERCOT_path)




def source_selection():
    # Use this function to eliminate duplicate entries such as 112 or 1233 etc
    def uniqueCharacters(str): 
        # If at any time we encounter 2 same characters, return false 
        for i in range(len(str)): 
            for j in range(i + 1,len(str)):  
                if(str[i] == str[j]): 
                    return False; 
        # If no duplicate characters encountered, return true 
        return True; 

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
    list1 = sourcetype
    return(list1)




# Holy Grail of ERCOT Web Scrape
def data_extract(user_path=None): 

    if user_path is None:
        setup = user_setup()
        ERCOT_path = setup[1]
        DataDict = setup[0]

    else:
        if os.path.isdir(str(user_path)+'\\ERCOT Data') == False: # Check that there actually is a directory with necessary folders
            newsetup = input('No folders found! \nRedirecting to user setup...\n \nWould you like to create the necessary folders in current directory ('+os.path.join(user_path,'')+') or would you like to choose another location and run setup assistant?\n  1) Create folders in '+user_path+' \n  2) Choose alternative location\n  3) Exit')
            
            if newsetup == str(1):
                tmp_path = dir_create(user_path)
                setup = user_setup(tmp_path)
                DataDict = setup[0]
                ERCOT_path = setup[1]

            elif newsetup == str(2):
                setup = user_setup()
                DataDict = setup[0]
                ERCOT_path = setup[1]
            
            elif newsetup == str(3):
                sys.exit('Leaving current state... \n\nSession aborted.')

            else:
                sys.exit('ERROR: Could not complete setup. Please try again.')
        else:
            
            ERCOT_path = user_path
            DataDict = DataDict_setup(ERCOT_path)

    list1 = source_selection()

    now = datetime.now()
    current_time = now.strftime("%H%M%S")
    #print("Current Time =", current_time)
    ex1 = ['output'+str(current_time)+'.txt']
    ex1[0]
    ercot = 'http://mis.ercot.com'
    
    xx = 0
    ii=0

    for xx in range(len(list1)):
        for ii in range(len(DataDict[list1[xx]])):
            resp = request.urlopen(DataDict[list1[xx]][ii][1])
            soup = BeautifulSoup(resp, from_encoding=resp.info().get_param('charset'),features="lxml")

            test = []

            for link in soup.find_all('a', href=True):
                test.append(str(ercot + link['href']))
                #print(link['href'])

            lengthURL = len(test)

            filelist = []
            for path in Path(DataDict[list1[xx]][ii][2]).rglob('*'):
                filelist.append(path.name)

            jj = 0
            for jj in range(lengthURL):
                response = requests.get(test[jj], verify = False)    # Added verify = False ..not sure if this is the correct fix or not. Test this.
                jj+=1
                zip = zipfile.ZipFile(io.BytesIO(response.content))
                if os.path.splitext(str(zip.namelist()))[1]==".xml']":
                    continue
                if zip.namelist()[0] in filelist:
                    break
                zip.extractall(DataDict[list1[xx]][ii][2])

            os.chdir(DataDict[list1[xx]][ii][2])
            MyFile=open(ex1[0],'w')
            test=map(lambda x:x+'\n', test)
            MyFile.writelines(test)
            MyFile.close()

            print('Completed iteration ',ii+1,'/',len(DataDict[list1[xx]]),' for ',list1[xx])

    print('Job complete')
    os.chdir(ERCOT_path)
    
# Extract Fuel Mix file from ERCOT
def fuel_mix_extract(ERCOT_path):
    os.chdir(str(ERCOT_path+'ERCOT Data/Generation/Fuel Mix'))

    dls = 'http://www.ercot.com/content/wcm/lists/181766/IntGenbyFuel2020.xlsx'
    resp = requests.get(dls)

    output = open('2020_Fuel_Mix.xlsx', 'wb')
    output.write(resp.content)
    output.close()




def combined_files_create(user_path):
    '''General function to take a path and create the necessary folders for combining the ERCOT Data
            Must specify path argument'''
    
    # include a function here that checks whether there is an 'ERCOT Data' directory or whether there needs to be one created

    if os.path.splitdrive(user_path)[1] == '\\':
        user_path = os.path.splitdrive(user_path)[0] 
    #elif os.path.split(user_path)[1] != '':
    #    path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 
    else:
        user_path = os.path.split(user_path)[0] + os.path.split(user_path)[1] 

    if os.path.isdir(str(user_path)+'\\ERCOT Data\\Combined Data') == False:
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data')

        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM')

        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\Energy Purchased & Sold')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\Energy Purchased & Sold\\Purchased')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\Energy Purchased & Sold\\Sold')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\LMP - Hourly DAM')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\PTP Obligations')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\PTP Option Prices')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\Shadow Prices')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\DAM\\SPP')

        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM\\Historical SPP - LZ & Hub')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM\\LMP - Electrical Bus')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM\\LMP - Resource Node - Load Zone - Hub')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM\\SPP')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Market Information\\RTM\\SCED Shadow Prices')

        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Fuel Mix')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind\\Historical Hourly Wind Output')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind\\IntraHour - Actual')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind\\IntraHour - Forecast')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind\\System Wide & LZ Wind Production - Act vs Fcast')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Generation\\Wind\\Wind Integration Reports')

        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Load')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Load\\Actual System Load - Weather Zone')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Load\\IntraHour Forecast - Weather Zone')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Load\\Real Time Load - Actual vs Forecast (Hourly)')
        os.mkdir(str(user_path)+'\\ERCOT Data\\Combined Data\\Load\\Seven-Day Load Forecast - Weather Zone')

    CombinedDict = {'DAM':[ ['Total Energy Purchased',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/Energy Purchased & Sold/Purchased'],
                            ['Total Energy Sold',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/Energy Purchased & Sold/Sold'],
                            ['DAM SPPs',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/SPP'],
                            ['Shadow Prices',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/Shadow Prices'],
                            ['PTP Obligations',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/PTP Obligations'],
                            ['PTP Option Prices',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/PTP Option Prices']],#],
                            #['Historical SPPs (Hourly) by Load Zone and Hub',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/Historical SPP - LZ & Hub']],

                        'RTM':[['RTM SPPs',user_path + '/ERCOT Data/Combined Data/Market Information/RTM/SPP'],
                                ['SCED Shadow Prices',user_path + '/ERCOT Data/Combined Data/Market Information/RTM/SCED Shadow Prices'],
                                ['Historical SPPs (15 min) by Load Zone and Hub',user_path + '/ERCOT Data/Combined Data/Market Information/RTM/Historical SPP - LZ & Hub']],

                        'LMP':[['DAM Hourly LMPs',user_path + '/ERCOT Data/Combined Data/Market Information/DAM/LMP - Hourly DAM'],
                                ['RTM Resource Node LMPs (5 min)',user_path + '/ERCOT Data/Combined Data/Market Information/RTM/LMP - Resource Node - Load Zone - Hub'],
                                ['RTM Electric Bus LMPs (5 min)',user_path + '/ERCOT Data/Combined Data/Market Information/RTM/LMP - Electrical Bus']],

                        'Load':[['Real Time Load - Actual vs Forecast (Hourly) (System Wide)',user_path + '/ERCOT Data/Combined Data/Load/Actual/Real Time Load - Actual vs Forecast (Hourly)'],
                                ['Actual System Load (Hourly by Weather Zone)',user_path + '/ERCOT Data/Combined Data/Load/Actual/Actual System Load - Weather Zone'], 
                                ['Intrahour Load Forecast (by Weather Zone)',user_path + '/ERCOT Data/Combined Data/Load/Actual/IntraHour Forecast - Weather Zone'],
                                ['Seven Day Load Forecast (Hourly by Weather Zone)',user_path + '/ERCOT Data/Combined Data/Load/Actual/Seven-Day Load Forecast - Weather Zone']],

                'Generation':[['System Wide & LZ Wind Power Production: Actual vs Forecast (Hourly)',user_path + '/ERCOT Data/Combined Data/Generation/Wind/System Wide & LZ Wind Production - Act vs Fcast'],
                                ['IntraHour Wind Production Actual - Load Zone',user_path + '/ERCOT Data/Combined Data/Generation/Wind/IntraHour - Actual'],
                                ['IntraHour Wind Production Forecast - Load Zone',user_path + '/ERCOT Data/Combined Data/Generation/Wind/IntraHour - Forecast']]
                    }

    return(CombinedDict)





def source_combine(user_path=None): # we are assuming directory exists and that there is data there...?
    if user_path is None:
        inputpath = input('Please enter path that has ercot data folder')
    else:
        inputpath = user_path
    
    from_path = DataDict_setup(inputpath) # <-- CHECK ..?? need to remove user_path ... its not specified
    combo_path = combined_files_create(inputpath)
    list1 = source_selection()
    
    #combo_path = combined_files_create(user_path)[1] #--> dont ALWAY need to create directory..fix
    extension = 'csv'

    for xx in range(len(list1)):
        for ii in range(len(combo_path[list1[xx]])):
        
            allfiles = from_path[list1[xx]][ii][2]
            os.chdir(allfiles)

            newfilename = combo_path[list1[xx]][ii][0] # <-- name for combined csv file   :begin loop here 
            newpath = combo_path[list1[xx]][ii][1] # <-- NEW file path for combined

            all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
            #combine all files in the list
            combined_csv = pd.concat([pd.read_csv(f, sep=",") for f in all_filenames ])

            os.chdir(newpath)
            #export to csv
            combined_csv.to_csv( test['DAM'][0][1] + "\\combined_"+str(test['DAM'][0][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    
    


    

# In[156]:


# For Reference:

# 00) dir_create('E:\\')
# 1A) manual_setup('E:\\')
# 1B) user_specified_setup()
#  2) ERCOT_extraction()
#  3) fuel_mix_extract(ERCOT_path)


# In[ ]:

def load_lmp_coords():
    df = pd.read_excel(r'E:/ERCOT/ERCOT Data/Other/SP_List_EB_Mapping/LMP Points and Coordinates.xlsx', sheet_name='LMP Points and Coordinates')
    df = df.loc[pd.notnull(df['Lat'])]
    df = df.sort_values('RESOURCE_NODE')
    return df

def DAM_combine(condense=0):

    # Right now, this only works for single selection (i.e. 1 or 2 or 3 or...) It does not work for 123 or 34 etc
    selection = input('(1) Energy Purchased \n(2) Energy Sold \n(3) PTP Obligations \n(4) PTP Options \n (5) Shadow Prices \n(6) SPP \n(7) LMP - Hourly')

    if selection == "":
        sys.exit()

    paths = DataDict_setup(ERCOT_path='E:/ERCOT')
    extension = 'csv'
    coord_map = load_lmp_coords()


    # For all energy purchased:
    if selection in str(1):
        purch_path = paths['DAM'][0][2]
        os.chdir(purch_path)
        all_purch_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_purch = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_purch_csvs])
        combined_purch['Transaction'] = 'Purchase'

        
        combined_purch['lat'] = combined_purch['Settlement_Point'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_purch['lon'] = combined_purch['Settlement_Point'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
        
        if condense == 1:
            combined_purch = combined_purch[combined_purch['lat'].notna()]

        print(combined_purch)
        #combined_purch.to_csv( os.path.dirname(purch_path) + "\\Combined\\combined_"+str(paths['DAM'][0][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_purch.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][0][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    # For all energy sold:
    if selection in str(2):
        sold_path = paths['DAM'][1][2]
        os.chdir(sold_path)
        all_sold_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_sold = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_sold_csvs])
        combined_sold['Transaction'] = 'Sold'

        combined_sold['lat'] = combined_sold['Settlement_Point'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_sold['lon'] = combined_sold['Settlement_Point'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
        
        if condense == 1:
            combined_sold = combined_sold[combined_sold['lat'].notna()]

        print(combined_sold)
        #combined_sold.to_csv( os.path.dirname(sold_path) + "\\Combined\\combined_"+str(paths['DAM'][1][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_sold.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][1][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    # For all PTP Obligations (Source vs Sink quantities)
    if selection in str(3):
        ptp_obl_path = paths['DAM'][4][2]
        os.chdir(ptp_obl_path)
        all_ptp_obl_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_ptp_obl = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_ptp_obl_csvs])
        combined_ptp_obl['Transaction'] = 'PTP Obligations'

        combined_ptp_obl['lat'] = combined_ptp_obl['STL_PNT'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_ptp_obl['lon'] = combined_ptp_obl['STL_PNT'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
        
        if condense == 1:
            combined_ptp_obl = combined_ptp_obl[combined_ptp_obl['lat'].notna()]

        print(combined_ptp_obl)
        #combined_ptp_obl.to_csv( os.path.dirname(ptp_obl_path) + "\\Combined\\combined_"+str(paths['DAM'][4][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_ptp_obl.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][4][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    # For all PTP Options (Source vs Sink price)
    if selection in str(4):
        ptp_option = paths['DAM'][5][2]
        os.chdir(ptp_option)
        all_ptp_option_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_ptp_option = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_ptp_option_csvs])
        combined_ptp_option['Transaction'] = 'PTP Options'

        combined_ptp_option['source_lat'] = combined_ptp_option['Source'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_ptp_option['source_lon'] = combined_ptp_option['Source'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
 
        combined_ptp_option['sink_lat'] = combined_ptp_option['Sink'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_ptp_option['sink_lon'] = combined_ptp_option['Sink'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
 
        # Not sure if I want to include this since there are two potential unknowns, the source lat/lon and sink lat/lon
        #if condense == 1:
        #    combined_ptp_option = combined_ptp_option[combined_ptp_option['source_lat'].notna()]
        #    combined_ptp_option = combined_ptp_option[combined_ptp_option['sink_lat'].notna()]

        print(combined_ptp_option)
        #combined_ptp_option.to_csv( os.path.dirname(ptp_obl_path) + "\\Combined\\combined_"+str(paths['DAM'][5][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_ptp_option.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][5][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    if selection in str(5):
        shadow_price = paths['DAM'][3][2]
        os.chdir(shadow_price)
        all_shadow_price_csvs = [i for i in glob.glob('*.{}'.format(extension))] 

        combined_shadow_prices = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_shadow_price_csvs])
        combined_shadow_prices['Transaction'] = 'Shadow Price'

# These are not unique substations so I will need to figure out another way to group them
#        combined_shadow_prices['incoming_lat'] = combined_shadow_prices['FromStation'].map(coord_map.set_index('SUBSTATION')['Lat'])
#        combined_shadow_prices['incoming_lon'] = combined_shadow_prices['FromStation'].map(coord_map.set_index('SUBSTATION')['Lon'])
# 
#        combined_shadow_prices['outgoing_lat'] = combined_shadow_prices['ToStation'].map(coord_map.set_index('SUBSTATION')['Lat'])
#        combined_shadow_prices['outgoing_lon'] = combined_shadow_prices['ToStation'].map(coord_map.set_index('SUBSTATION')['Lon'])
 
        print(combined_shadow_prices)
        #combined_shadow_prices.to_csv( os.path.dirname(ptp_obl_path) + "\\Combined\\combined_"+str(paths['DAM'][3][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_shadow_prices.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][3][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')



    # For all Settlement Point Prices (SPP)
    if selection in str(6):
        spp_path = paths['DAM'][2][2]
        os.chdir(spp_path)
        all_spp_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_dam_spp = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_spp_csvs])
        combined_dam_spp['Transaction'] = 'SPP'

        combined_dam_spp['lat'] = combined_dam_spp['SettlementPoint'].map(coord_map.set_index('RESOURCE_NODE')['Lat'])
        combined_dam_spp['lon'] = combined_dam_spp['SettlementPoint'].map(coord_map.set_index('RESOURCE_NODE')['Lon'])
        
        if condense == 1:
            combined_dam_spp = combined_dam_spp[combined_dam_spp['lat'].notna()]

        print(combined_dam_spp)
        #combined_dam_spp.to_csv( os.path.dirname(ptp_obl_path) + "\\Combined\\combined_"+str(paths['DAM'][4][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_dam_spp.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['DAM'][2][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')




    # For all LMP - Hourly (for DAM)
    if selection in str(7):
        lmp_path = paths['LMP'][0][2]
        os.chdir(lmp_path)
        all_lmp_csvs = [i for i in glob.glob('*.{}'.format(extension))]

        combined_dam_lmp = pd.concat([pd.read_csv(f, sep=",", header=0, skipinitialspace=True) for f in all_lmp_csvs])
        combined_dam_lmp['Transaction'] = 'LMP (DAM)'

        combined_dam_lmp['lat'] = combined_dam_lmp['BusName'].map(coord_map.set_index('ELECTRICAL_BUS')['Lat'])
        combined_dam_lmp['lon'] = combined_dam_lmp['BusName'].map(coord_map.set_index('ELECTRICAL_BUS')['Lon'])
        
        if condense == 1:
            combined_dam_lmp = combined_dam_lmp[combined_dam_lmp['lat'].notna()]

        print(combined_dam_lmp)
        #combined_dam_lmp.to_csv( os.path.dirname(ptp_obl_path) + "\\Combined\\combined_"+str(paths['LMP'][0][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')
        combined_dam_lmp.to_csv( "E:\\ERCOT\\ERCOT Data\\Combined Data\\DAM\\combined_"+str(paths['LMP'][0][0]).replace(" ","")+".csv", index=False, encoding='utf-8-sig')


# %%

import networkx as nx
import matplotlib as plt




# %%
