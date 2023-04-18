### General
The folder contains 3 scripts which were requested to implement as part of HW7

### task_1 
This folder contains 2 python executable files: 
1. _query_1.py_
2. _query_2.py_

### How to run the scripts

1. Basic usage: 
```python
python3 hw_7_spark/task_1/query_1.py
python3 hw_7_spark/task_1/query_2.py
```

### Deliverables 
All the results are stored in a separate folder which are defined in the **properties.ini** file. By default, 
all the results are being stored in **hw_7_spark/results/task{N}/** folder 


### task_2 
The folder contains only _main.py_ script. 

### How to run the script

1. Basic usage: 
```python
python3 hw_7_spark/task_2/main.py
```

### Deliverables 
No Deliverables as per requirements. The output is just being printed into console


## Test data
There's **hw_7_spark/testdata** folder which contains the following files: 
1. Testdata (_airlines.csv_, _airlines.json_). 
<span style="color: red;">IMPORTANT NOTE.</span> AS _flights.csv_  HAS SIZE WHICH EXCEEDS 500MB - IT IS NOT POSSIBLE TO PUSH IT TO GITHUB. SO THIS FILE MUST BE DOWNLOADED FROM _https://www.kaggle.com/datasets/usdot/flight-delays_ AND PLACED MANUALLY TO _**/testdata/**_ FOLDER
2. Gold data - data stored in _.parquet_ files which is used during comparison/assertion