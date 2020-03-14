import json 
import sys
# non-standard libs
import pandas as pd
import requests
import shutil

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
def scrape_image_given_link(image_url, outputfile)
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


def main():
    # import the json file
    df = pd.read_json(sys.argv[1], lines=True)
    df2 = pd.DataFrame()

    # grabbing sub information from "content"
    for row in range(df.shape[0]):
        temp_content_dict = dict(df.loc[row]["content"])

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

        # grabbing either the keys or the values.
        #print([dict_fmt[i] for i in range(0, len(dict_fmt), 2)])
    
    df = df.drop("content",axis=1)
    df.to_csv(r'./righthere_2.csv')
    df2.to_csv(r'./righthere.csv')

if __name__ == "__main__":
    main()
