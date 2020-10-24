
# A - B Test Platform

##### Key Features

-   allows you to find the Dsitribution of the testing values.
-   Time period detection (year, quarter, month, week, week-part, day, hour) adding as subgroups
-   subgroups og testing is available
-   schedule your test daily, monthly, weekly, hourly.
-   Confidence level can aoutmatically assigns and tests for each Confidencce levels (e.g. 0.01, 0.05 appying for testes individually)

##### Running Platform

- **Test Parameters**
    
    ***test_groups :*** if there are any sub-groups of the active and control group, framework can run Test results for each subgroups. This parameter must be the column name which exits on given data set for both Active and Control groups.
    
    **groups :*** The column name represents active and control group flag.
    
    **date :*** If it needs, it is able to be trigger related to a date that data is goint to be filtered as before the given date.
    
    **feature :*** the column name that represents actual values that are tested according to two main groups.
    
    **data_source :*** The location where the data is stored or the query (checck data source for details).
    
    **data_query_path :*** type of data source to import data to the platform (optinal Ms SQL, PostgreSQL, AWS RedShift, Google BigQuery, csv, json, pickle).
    
    **time_period :*** additional time period which c(optinal year, month, day, hour, week, week day, day part quarter) (check details time periods).
    
    **time_indicator :*** If test is running periodicly, the column name that related to time must be assigned.
    
    **export_path :*** Output results of export as csv format (optional).
    
    **connector :*** if there is a connection paramters as user, pasword, host port, this allows us to assign it as dictionary format (e.g {"user": ***, "pw": ****}).
    
    **confidence_level :*** Confidence level of test results (list or float).
    
    **boostrap_sample_ratio :*** Boostraping randomly selected sample data raio (between 0 and 1).
    
    *boostrap_iteration :*** Number of iteration for boostraping.
    
    
    

##### Data Source
Here is the data source that you can connect with your SQL queries:

- Ms SQL Server
- PostgreSQL
- AWS RedShift
- Google BigQuery
- .csv
- .json
- pickle
    
-   ***Connection PostgreSQL - MS SQL - AWS RedShift***
    
        data_source = "postgresql"
        connector = {"user": ***, "password": ***, "server": "127.0.0.1", 
                     "port": "5440", "db": ***}
        data_main_path ="""
                           SELECT                             
                            groups,
                            test_groups
                            feature,
                            time_indicator
                           FROM table
                       """
        
        
-   ***Connection Google BigQuery***
        
        data_source = "googlebigquery"
        connector = {"data_main_path": "./json_file_where_you_stored", 
                     "db": "flash-clover-*********.json"}
        data_main_path ="""
                   SELECT                             
                    groups,
                    test_groups
                    feature,
                    time_indicator
                   FROM table
               """

-   **Connection csv - .json - .pickle** 
        
        data_source = "csv"
        data_main_path = "./data_where_you_store/***.csv"