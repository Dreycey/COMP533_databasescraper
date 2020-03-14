import json 
import sys
import pandas as pd

"""
input_file = open(sys.argv[1], 'r')
for line in input_file.readlines():
    y = dict(line)
"""
#y = json.load(json_file)
"""
for k, v in y.items():
    print (k,v)

print (y['url'])
"""

df = pd.read_json(sys.argv[1], lines=True)

# save the dataframe to csv
#df.to_csv(r'./righthere.csv')

# print the major columns of the JSON
#for col in df.columns:
#    print(col)


# function used from https://stackoverflow.com/questions/46845464/cleaner-way-to-unpack-nested-dictionaries
def extract_nested_values(it):
    """
    This function is used to conver a nested dictionary
    into a list of values. 

    INPUT: nested dictionary
    OUT: A unpacked list
    """

    if isinstance(it, list):
        for sub_it in it:
            yield from extract_nested_values(sub_it)
    elif isinstance(it, dict): 
        for value in it.values():
            yield from extract_nested_values(value)
    else:
        yield it

# modified function to grab titles
def grab_column_names(it, keyin=''):
    """
    Purpose is to to return the keys into a list.

    IN: nestede dict
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

# grabbing sub information from "content"
for row in range(1):#df.shape[0]):
    temp_content_dict = dict(df.loc[row]["content"])
    extract_nested_values(temp_content_dict)
    


    #for output in keys:
    #    print(temp_content_dict[output])


#print(list(extract_nested_values(temp_content_dict)))

# getting output into a dict fmt
dict_fmt = list(grab_column_names(temp_content_dict))

document_dict = {}
for index in range(0, len(dict_fmt), 2):
    k_val = dict_fmt[index]
    valu = dict_fmt[index + 1]
    if dict_fmt[index] not in document_dict.keys():
        document_dict[k_val] = valu
    else:
        cur_val = document_dict[k_val]
        new_val = f"{cur_val.strip('{').strip('}')},{valu}"
        document_dict[k_val] = '{' + new_val + '}'

df2 = pd.DataFrame([document_dict])
dict_fmt_2 = list(grab_column_names(document_dict))
print ([dict_fmt_2[i] for i in range(1, len(dict_fmt_2), 2)])
print (df2)
#print (temp_content_dict)


df2.to_csv(r'./righthere.csv')
