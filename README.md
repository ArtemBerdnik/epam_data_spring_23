### General
The script implements the following functionality: 
1. to read the data available in the given named node within a hadoop cluster
2. to convert the data to the avro, preserving initial datatypes
3. to write converted data back to the named node


### How to run the script

1. Basic usage: 
```python
python3 main.py
```

2. Run with optional parameters
```python
python3 main.py -origin_file=super_duper_new_file.dat -destination_file=awesome_results.avro -file_workflow=delete
```

### Optional parameters explanation
The script accepts the following parameters that are optional:
1. **-origin_file**. What file to read the data from. Default value is airlines.dat
2. **-destination_file**. What file to store the convered data into
3. **-file_workflow**. How to store data in the hadoop. There're 2 options:
- _'keep'_ - means that if the file with a given name already exists in the hadoop, it will not get overwritten, instead 
a new file with a _{current_date} prefix in the name will be created
- _'delete'_ - means that if file with a given name exists it will be deleted before processing

### Other 
1. Main configuration is being stored in properties.ini 