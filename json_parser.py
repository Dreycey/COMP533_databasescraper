import json 
import sys
import os
from os import listdir
from os.path import isfile, join
# non-standard libs
import pandas as pd
import requests
import shutil
from tqdm import tqdm
import numpy as np

"""
This is a parsing script made for parsing through files
that contain metadata for the Smithsonian OpenAccess.

INPUT: file with multiple JSON objects. 

OUTPUT: .csv file that can be uploaded into postgres
"""



# modified function to grab titles
# modified from from https://stackoverflow.com/questions/46845464/cleaner-way-to-unpack-nested-dictionaries
def grab_column_names(it, keyin=''):
    """
    Purpose is to to return the keys into a list.

    IN: nested dict
    OUT: list of the keys
    """
    if isinstance(it, list):
        for sub_it in it:
            if keyin != '':
                yield from grab_column_names(sub_it, keyin)
            else:
                yield from grab_column_names(sub_it)
    elif isinstance(it, dict): 
        for key, value in it.items():
            if keyin != '':
                key = keyin + '_' + key
            yield from grab_column_names(value, key)
    else:
        yield keyin
        yield it

# store "content" into an unpack dictionary with multiple values
# stored into a postgres array format. 
# NOTE: this is NOT in a generalized format. Should turn into a 
#       data structure that can be used in multiple databases. 
#       This could be turned into a function with input being a 
#       dictionary and the output being an input for different
#       database types. Have flexibility here is key. 
def make_postgres_database(key_value_list):
    """
    This function is used to make a dictionary format that
    can be later used as input into a panda dataframe or 
    for direct printing. 

    INPUT: A list with [key_1, value_1, ..., key_n, value_n]
    
    OUTPUT: dictionary with key duplicates storing values
           compatable with the postgres array format. 
    """
    document_dict = {}
    for index in range(0, len(key_value_list), 2):
        k_val = key_value_list[index]
        valu = key_value_list[index + 1]
        if key_value_list[index] not in document_dict.keys():
            document_dict[k_val] = valu
        else:
            cur_val = document_dict[k_val]
            new_val = f"{cur_val.strip('{').strip('}')},{valu}"
            document_dict[k_val] = '{' + new_val + '}'
    return document_dict

# function for scraping the images given URL and output
def scrape_image_given_link(image_url, outputfile):
    """
    This function takes in an image URL and saves the image
    locally. This can be used to scrape the images for a 
    given input. 

    INPUT: image URL and path to saved file. 

    OUTPUT: path to saved file. But more importantly,
            the image is saved to that destination. 
    """

    # Open the url image, set stream to True, this will return the stream content.
    resp = requests.get(image_url, stream=True)
    # Open a local file with wb ( write binary ) permission.
    local_file = open(outputfile, 'wb')
    # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
    resp.raw.decode_content = True
    # Copy the response stream raw data to local image file.
    shutil.copyfileobj(resp.raw, local_file)
    # Remove the image url response object.
    ## not sure this is needed

def grab_sub_information(pandas_dataframe, column_name):
    """
    This function grabs a nested json attribute and
    unpacks all of the values, returning a dataframe
    with all subvalues added into one row. It goes through
    the entire data frame and does this row-by-row.

    INPUT: dataframe with sub column to be parsed. 

    OUTPUT: dataframe with sub columns all parsed. 
    """
    df2 = pd.DataFrame()
    # grabbing sub information from "content"
    if column_name in pandas_dataframe:
        for row in range(pandas_dataframe.shape[0]):
            temp_content_dict = dict(pandas_dataframe.loc[row][column_name])
            # getting output into a dict fmt
            dict_fmt = list(grab_column_names(temp_content_dict))
            # Outputing the postgres dictionary.
            document_dict_in = make_postgres_database(dict_fmt)
            df2_temp = pd.DataFrame([document_dict_in]) # [] around the dict makes it a row
            # add temp to the full dataframe
            if df2.shape[0] == 0:
                df2 = df2_temp
            else:
                df2 = pd.concat([df2, df2_temp])
    else:
        print(f"WARNING: column name {column_name} isn't in input")
    return df2;

def make_subbash(dataframe_in, output_shell_name, table_name, csv_output_location):
    """
    This function takes in a dataframe and returns a 
    bash script that can be used to build the postgres
    database from the terminal. 

    INPUT: dataframe and the output shell name

    OUTPUT: bash script that can be used to build the database
    """
    # evaluate information about the dataframe
    df_keys = dataframe_in.keys()
    make_table_string = f'psql -d $database -c "CREATE TABLE {table_name}('
    for df_k in df_keys:
        df_v = dataframe_in[dataframe_in[df_k].notnull()][df_k]
        df_v = df_v.sample(1).values[0]
        print(f"now looking at {df_k}, {type(df_v)}") 
        df_k = df_k.strip(',')
        if isinstance(df_v, np.int64):
            print('PSQL INT #####################')
            make_table_string = make_table_string + f"{df_k} int"
        elif isinstance(df_v, np.datetime64):
            print('PSQL dataframe #################')
            make_table_string = make_table_string + f"{df_k} timestamp"
        elif isinstance(df_v, np.float64):
            print('PSQL float #################')
            make_table_string = make_table_string + f"{df_k} float"
        elif isinstance(df_v, str):
            if '{' in df_v:
                print ('PSQL array ##############')
                make_table_string = make_table_string + f"{df_k} text[]" 
            else:
                print ('PSQL varchar[100] ##############')
                make_table_string = make_table_string + f"{df_k} text" 
        make_table_string = make_table_string + ","
    make_table_string = make_table_string[:-1] + ")"
    # make the command for building the table
    build_table_command = f"psql -d $database -c \"\copy {table_name} FROM \
    '{csv_output_location}' DELIMITER ',' CSV\""
    #####
    # Adding to the file
    #####
    # add the table to the new bash script
    # kludge: pass pointer to this file to keep from having to reopen
    newbashfile = open(output_shell_name, 'a')
    newbashfile.write('\n')
    newbashfile.write(make_table_string)
    newbashfile.write('\n')
    newbashfile.close()
    # add commands for building the table


def main(directory_path, output_name, bash_out_name):
    subfiles = [join(directory_path, sub_file) for sub_file in
                listdir(directory_path) if isfile(join(directory_path,
                sub_file))]
    main_df = pd.DataFrame()
    main_df2 = pd.DataFrame()
    print (f"Loading all of the files for the directory now")
    for subfile_path in tqdm(subfiles):
        # import the json file
        df = pd.read_json(subfile_path, lines=True)
        # scrape through the sub attribute for 'content'
        df2_temp = grab_sub_information(df, "content")
        # save file dataframes to the main df for the dir
        main_df = pd.concat([main_df, df])
        main_df2 = pd.concat([main_df2, df2_temp])
    # drop the "content" attribute from the _main table
    has_content_boolean = False #initializing
    if "content" in main_df:
        main_df = main_df.drop("content",axis=1)
        has_content_boolean = True
    # make the sub-bash script for psql
    base_name_folder = os.path.basename(os.path.normpath(output_name))
    make_subbash(main_df,
                 bash_out_name,
                 f'{base_name_folder}_main',
                 f'./{output_name}_main.csv')
    if has_content_boolean:
        make_subbash(main_df2,
                     bash_out_name,
                     f'{base_name_folder}_content',
                     f'./{output_name}_content.csv')
    # save everything to csv
    if has_content_boolean:
        main_df2.to_csv(f'./{output_name}_content.csv')
    main_df.to_csv(f'./{output_name}_main.csv')

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
