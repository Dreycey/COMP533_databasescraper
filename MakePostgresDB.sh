#!/bin/bash
#
# build postgres DB ussing simthsonian OpenAccess data

# Download OpenAcess data
if [ ! -d 'OpenAccess' ]
then
    echo "Donloading data, please wait.. \n"
    git clone https://github.com/Smithsonian/OpenAccess;
fi;

# make storage area for the input to poostgres
if [ ! -d 'postgress_DB' ]
then
    mkdir postgress_DB;
fi;

# transform all of the json files in OpenAccess into csvs
for subdirectory in OpenAccess/metadata/objects/*;
do
    echo $subdirectory;
    subdir_name=$(basename $subdirectory);
    output_dir='postgress_DB/'$subdir_name;
    echo $output_dir;
    if [ ! -d $output_dir ]
    then    
        echo Now working on $subdir_name;
        mkdir $output_dir;
        python json_parser.py $subdirectory $output_dir/$subdir_name;
    fi;
done;

# build the database running the subscript made above
subbash_script_name='tehe' 
if [ ! -f 'postgress_DB' ]
then
    echo ERROR: No bash script found matching $subbash_script_name;
else
    bash $subbash_script_name;
fi;


