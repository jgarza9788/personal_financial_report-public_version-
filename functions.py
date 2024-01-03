##import the Libraries

# standard Libraries
import os
import re
import json5 as json 
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# used for displaying data and stuff
from IPython.display import display, HTML 

# used for charts and Graphs
import plotly
import plotly.express as px
import seaborn as sns


## set pandas options

# show all the columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# turn off a warning
pd.options.mode.chained_assignment = None  # default='warn'

## VARIABLES

DIR = os.getcwd()
data_DIR = os.path.join(DIR,'data')
old_data_DIR = os.path.join(DIR,'old_data')

if os.path.exists(data_DIR) == False:
    os.mkdir(data_DIR)

this_year = int(datetime.now().strftime("%Y"))
this_month = int(datetime.now().strftime("%m"))

# print(this_year,this_month)


def old_data_to_new_format():
    """
    takes some of my old files and converts them to the new format
    """
    old_files = []
    
    # there are two files
    old_files.append(os.path.join(old_data_DIR,'finances_2007_2017.xlsx'))
    old_files.append(os.path.join(old_data_DIR,'finances.xlsm'))

    df_old = pd.DataFrame()

    # combining the files
    for f in old_files:
        temp = pd.read_excel(f,sheet_name='HistoryDownload.csv')
        df_old = pd.concat([df_old,temp])

    # making new columns and formatting others
    df_old['Tags'] = ''
    df_old.loc[ df_old['CabinPayback'] == 1.0, 'Tags'] += 'CabinPayback,' 
    df_old.loc[ df_old['MovingExpense'] == 'x', 'Tags'] += 'MovingExpense,' 


    temp['Category'] = temp['Category'].str.upper()
    
    df_old['YYYY'] = pd.to_datetime(df_old['Date']).dt.strftime("%Y")
    df_old['YYYYMMDD'] = pd.to_datetime(df_old['Date']).dt.strftime("%Y%m%d")
    df_old['YYYYMM'] = pd.to_datetime(df_old['Date']).dt.strftime("%Y%m")
    df_old['YYYY.W'] = pd.to_datetime(df_old['Date']).dt.strftime("%Y.%W")

    df_old['Date'] = pd.to_datetime(df_old['Date']).dt.strftime("%Y-%m-%d")

    df_old['YYYY'] = pd.to_numeric(df_old['YYYY'],errors='coerce')
    df_old['YYYY'] = df_old['YYYY'].astype(int,errors='ignore')

    df_old['YYYYMMDD'] = pd.to_numeric(df_old['YYYYMMDD'],errors='coerce')
    df_old['YYYYMMDD'] = df_old['YYYYMMDD'].astype(int,errors='ignore')

    df_old['YYYYMM'] = pd.to_numeric(df_old['YYYYMM'],errors='coerce')
    df_old['YYYYMM'] = df_old['YYYYMM'].astype(int,errors='ignore')

    df_old['YYYY.W'] = pd.to_numeric(df_old['YYYY.W'],errors='coerce')
    df_old['YYYY.W'] = df_old['YYYY.W'].astype(float,errors='ignore')

    # these will be the final columns
    df_old = df_old[['Date','YYYYMMDD','YYYYMM','YYYY.W','YYYY','Location','Card','Delta','Balance','Category','Tags']]

    df_old['Delta'] = pd.to_numeric(df_old['Delta'],errors='coerce')
    df_old['Delta'] = df_old['Delta'].astype(float,errors='ignore')

    df_old['Balance'] = pd.to_numeric(df_old['Balance'],errors='coerce')
    df_old['Balance'] = df_old['Balance'].astype(float,errors='ignore')
    
    year_list = list(df_old.YYYY.drop_duplicates())

    for y in year_list:
        temp = df_old[df_old.YYYY == y]
        temp.to_csv(os.path.join(data_DIR, 'data_' + str(y) + '.csv'),index=False)


def process_new_data(new_csv=r'C:\Users\JGarza\Downloads\stmt.csv'):
    """
    process the newest match of transactions
    """
    
    # removes the header/summary
    lines = None
    with open(new_csv, 'r') as f:
        lines = f.readlines()
    if lines[0] == 'Description,,Summary Amt.\n':
        with open(new_csv, 'w') as f:
            f.writelines(lines[6:])

    # turn into a dataframe
    temp = pd.read_csv(new_csv)

    # reformat columns and stuff
    
    # print(temp.dtypes)
    
    temp['Location'] = temp['Description']

    #clean location
    temp['Location'] = temp['Location'].astype(str).str.replace(',', '')
    temp['Location'] = temp['Location'].astype(str).str.replace(';', '')
    
    temp['Delta'] = temp['Amount']
    temp['Balance'] = temp['Running Bal.']

    if 'Tags' not in temp.columns:
        temp['Tags'] = ''

    if 'Category' not in temp.columns:
        temp['Category'] = ''

    if 'Card' not in temp.columns:
        temp['Card'] = 'BankOfAmerica-Debit'
    
    temp['YYYY'] = pd.to_datetime(temp['Date']).dt.strftime("%Y")
    temp['YYYYMMDD'] = pd.to_datetime(temp['Date']).dt.strftime("%Y%m%d")
    temp['YYYYMM'] = pd.to_datetime(temp['Date']).dt.strftime("%Y%m")
    temp['YYYY.W'] = pd.to_datetime(temp['Date']).dt.strftime("%Y.%W")
    
    temp['Date'] = pd.to_datetime(temp['Date']).dt.strftime("%Y-%m-%d")

    temp['YYYY'] = pd.to_numeric(temp['YYYY'],errors='coerce')
    temp['YYYY'] = temp['YYYY'].astype(int,errors='ignore')

    temp['YYYYMMDD'] = pd.to_numeric(temp['YYYYMMDD'],errors='coerce')
    temp['YYYYMMDD'] = temp['YYYYMMDD'].astype(int,errors='ignore')

    temp['YYYYMM'] = pd.to_numeric(temp['YYYYMM'],errors='coerce')
    temp['YYYYMM'] = temp['YYYYMM'].astype(int,errors='ignore')

    temp['YYYY.W'] = pd.to_numeric(temp['YYYY.W'],errors='coerce')
    temp['YYYY.W'] = temp['YYYY.W'].astype(float,errors='ignore')

    # these will be the final columns... and their order
    temp = temp[['Date','YYYYMMDD','YYYYMM','YYYY.W','YYYY','Location','Card','Delta','Balance','Category','Tags']]

    try:
        temp['Delta'] = temp['Delta'].astype(str).str.replace(',', '')
    except:
        pass
    
    temp['Delta'] = pd.to_numeric(temp['Delta'],errors='coerce')
    temp['Delta'] = temp['Delta'].astype(float,errors='ignore')
    temp['Delta'] = temp['Delta'].fillna(0.0)

    temp['Balance'] = temp['Balance'].str.replace(',', '')
    temp['Balance'] = pd.to_numeric(temp['Balance'],errors='coerce')
    temp['Balance'] = temp['Balance'].astype(float,errors='ignore')

    return temp 


def load_last_x_years(x,verbose=False):
    """
    used to load multiple years of transactions
    """
    files = os.listdir(data_DIR)
    files.sort()
    files.reverse()

    if verbose:
        print(files)

    result = pd.DataFrame()

    for file in files[:x]:
        if verbose:
            print(file)
        file = os.path.join(data_DIR, file)
        result = pd.concat([result, pd.read_csv(file)])

    result = result.sort_values(by = 'YYYYMMDD',ascending=False)
    result = result.reset_index(drop=True)

    return result

# test 
# load_last_x_years(10,verbose=True)


def load_year(year):
    """
    loads one year of data
    """
    try:
        file = os.path.join(data_DIR,'data_' + str(year) + '.csv')
        return pd.read_csv(file)
    except:
        print('Happy New Years!')
        


# x = load_year(this_year)
# display(x)

def get_all_data(verbose = False):
    """
    gets all the years of data
    """
    files = os.listdir(data_DIR)
    files.sort()
    files.reverse()

    result = pd.DataFrame()

    # looks through all the files and combine them
    for file in files:
        try:
            if verbose:
                print(file)
            ffp = os.path.join(data_DIR, file)
            temp = pd.read_csv(ffp)

            # if verbose:
            #     print(*temp.columns,sep=', ')

            result = pd.concat([result, temp])
        except:
            print(file)

    return result

# # used for testing
# x = get_df_all(verbose=False)

def open_year_in_excel(y):
    """
    loads one year of data
    """
    try:
        file = os.path.join(data_DIR, 'data_' + str(y) + '.csv')
        os.system(r' start excel ' + file )
    except Exception as ex:
        print(str(ex))
        print('that year might not be available')


# edit_year(2020)

def open_year_in_scalc(y):
    """
    loads one year of data
    """
    try:
        file = os.path.join(data_DIR, 'data_' + str(y) + '.csv')
        os.system(r' start scalc ' + file )
    except Exception as ex:
        print(str(ex))
        print('that year might not be available')

def save_data(idf,verbose=False):
    """
    saves out the data ...in their own year file
    """
    year_list = list(idf.YYYY.drop_duplicates())

    for y in year_list:
        if verbose:
            print('Y: ', y)
        temp = idf[idf.YYYY == y]
        file = os.path.join(data_DIR, 'data_' + str(int(y)) + '.csv')
        temp.to_csv(file,index=False)
        if verbose:
            print('saved: ',file)

# def test_save_data():
#     df = load_year(2023)
#     save_data(df,verbose=True)
# test_save_data()            
            

def clean_loc(s):
    """
    takes in a string and clean it up...
    """
    s = s.upper()
    s = re.sub('#\d+','',s)
    s = re.sub('\d\d/\d\d','',s)
    s = re.sub('\d{2,10}','',s)
    s = re.sub('X{3,10}','',s)
    s = re.sub('\s+',' ',s)
    s = re.sub(r'(\\|/|\*)',' ',s)
    s = re.sub(r'(JUSTIN|GARZA|SQ|PAYPAL|PURCHASE)',' ',s)
    s = re.sub(r' (NC|CHARLOTTE)',' ',s)
    s = re.sub(r' (CA|CHATSWORTH)',' ',s)
    s = re.sub(r' . ',' ',s)
    s = re.sub('\s+',' ',s)
    s = re.sub(r'(-|#|:|,|\.com|\.|\'|\$|;)','',s)
    
    return s


def get_category_table(idf):
    """
    creates a lookup table.
    where the clean_loc matches (enough) the transaction will have the same Category
    """
    temp = idf.copy()
    temp['clean_loc'] = temp['Location']
    # temp = temp[['clean_loc','Location','Category']].drop_duplicates()
    temp = temp[['clean_loc','Category']] #.drop_duplicates()
    temp = temp[temp['Category'] != '']
    # temp = temp.drop_duplicates()
    temp = temp.dropna()

    temp['clean_loc'] = temp['clean_loc'].apply(clean_loc)

    temp['count'] = 1

    temp = pd.pivot_table(
        temp,
        index=['clean_loc','Category'],
        values='count',
        aggfunc={'count':sum}
        )
    temp = temp.reset_index()
    temp = temp.sort_values(by='count',ascending=False)
    temp = temp.reset_index()


    temp = temp[temp['count'] > 1 ]
    
    return temp

# #test
# temp = get_category_table(load_last_x_years(2))
# print(len(temp))
# display(temp)

def is_over(A,B,threshold=0.85,verbose=False):
    """
    does fuzzy matching ... anything over the threshold is considered a match
    """
    from difflib import SequenceMatcher as SM
    r = SM(isjunk=None, a=A, b=B).ratio()

    if r >= threshold:
        if verbose:
            print(A,'|',B,'|',r)
        return True
    else:
        return False

# print(is_over('yustin','justin',0.5,True)) 

def fill_in_category(df1,threshold=0.80):
    """
    takes in a dataframe and fills in the Category columns
    """
    # this category_table is made based on the last two years of transactions.
    # new locations and types of transactions will be filled in manually.
    catt = get_category_table(load_last_x_years(2))
    df1['clean_loc'] = df1.Location.apply(clean_loc)
    
    for i in df1.iterrows():
        if i[1]['Category'] == '':
            for c in catt.iterrows():
                # see if they match ... enough
                if is_over(i[1]['clean_loc'],c[1]['clean_loc'],0.80,False) == True:
                    df1.at[i[0],'Category'] = c[1]['Category']

    df1 = df1.drop(columns=['clean_loc'])
    return df1


def incorporated_data(current_data = None, new_data = None, autofillcat= True):
    """
    this will merge the new data with the current data
    """

    if current_data == None:
        try:
            current_data = load_year(this_year)
        except Exception as ex:
            current_data = load_year(this_year-1)
        except Exception as ex:
            print(str(ex))
            print('error loading new data')
            return None            

    
    if new_data == None:
        try:
            new_data = process_new_data()
        except Exception as ex:
            print(str(ex))
            print('error loading new data')
            return None
    
    result = pd.concat([current_data,new_data])
    
    # drop duplicates after the merge 
    result = result.drop_duplicates(['YYYYMMDD','Location','Delta','Balance'])
    
    # sort the values 
    result = result.sort_values(by='YYYYMMDD', ascending = False)

    # fill in the category 
    result.loc[result['Category'].isnull(),'Category'] = ''
    result = result.fillna('')
    if (autofillcat):
        result = fill_in_category(result,0.85)
    

    result['YYYYMMDD'] = result['YYYYMMDD'].astype(int)

    # date_format = '%m/%d/%Y'
    result['Date'] = pd.to_datetime(result['Date'],format='mixed')
    # result['Date'] = pd.to_datetime(result['Date'],format='%m/%d/%Y')
    # result['Date'] = pd.to_datetime(result['Date'],format='%Y-%m-%d')
    
    # drop duplicates again
    result = result.drop_duplicates(['YYYYMMDD','Location','Delta','Balance'])
    
    save_data(result)

    # return result

## used for testing
# x = incorporated_data()
# display(x)




# df_cat_by_m = dfy.groupby(['YYYYMM','Category']).sum()
# df_cat_by_m =  df_cat_by_m.reset_index(drop=False)
# df_cat_by_m['YYYYMM'] = df_cat_by_m['YYYYMM'].astype(int).astype(str)
# df_cat_by_m = df_cat_by_m.drop(columns=['YYYYMMDD','YYYY.W','YYYY','Balance'])
# # display(df_cat_by_m.head())