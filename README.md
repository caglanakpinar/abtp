
# A - B Test Platform

---------------------------

[![PyPI version](https://badge.fury.io/py/abtest.svg)](https://badge.fury.io/py/abtest)
[![GitHub license](https://img.shields.io/github/license/caglanakpinar/abtp)](https://github.com/caglanakpinar/abtp/blob/master/LICENSE)

----------------------------

##### Key Features

-   allows you to find the Distribution of the testing values.
-   Time period detection (year, quarter, month, week, week-part, day, hour) adding as subgroups.
-   subgroups of testing are available
-   schedule your test daily, monthly, weekly, hourly.
-   The confidence level can automatically assign and tests for each Confidence levels (e.g. 0.01, 0.05 applying for testes individually)

##### Running Platform

- **Test Parameters**
    
    ***test_groups :*** if there are any sub-groups of the active and control group, the framework can run Test results for each subgroup. This parameter must be the column name that exists on the given data set for both Active and Control groups.
    
    **groups :*** The column name represents the active and controls group flag.
    
    **feature :*** The column name that represents actual values that are tested according to two main groups.
    
    **data_source :*** The location where the data is stored or the query (check data source for details).
    
    **data_query_path :*** Type of data source to import data to the platform (optional Ms SQL, PostgreSQL, AWS RedShift, Google BigQuery, csv, json, pickle).
    
    **time_period :*** The additional time period which (optional year, month, day, hour, week, week day, day part quarter) (check details time periods). **This parameter must be assigned when A/B Test is scheduling**.
    
    **time_indicator :*** If test is running periodically, the column name that related to time must be assigned. **This parameter must be assigned when A/B Test is scheduling**.
    
    **export_path :*** Output results of export as csv format (optional). only path is enough for importing data with .csv format. Output will be '<date>_results.csv' with the test executed date. e.g. 20201205.results.csv
    
    **connector :*** if there is a connection paramters as user, pasword, host port, this allows us to assign it as dictionary format (e.g {"user": ***, "pw": ****}).
    
    **confidence_level :*** The Confidence level of test results (list or float).
    
    **boostrap_sample_ratio :*** Bootstrapping randomly selected sample data rate (between 0 and 1).
    
    *boostrap_iteration :*** Number of iteration for bootstrapping.
    
    ***time_schedule :*** When AB Test need to be scheduled, the only period of time is required.  Available time periods are 'Hourly', 'Monthly', 'Weekly', 'Mondays', ... , Sundays..
    **This parameter must be assigned when A/B Test is scheduling**.
    


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
        
      It is crucial that when data source is assigned as 'csv' - 'json' - 'pickle', file path must be assigned directly to file with the format. 
      For instance data_source is 'csv' and 'data_main_path must be '/data_where_you_store/data_where_you_store_2/../data_that_you_want_to_import.csv'
        
        data_source = "csv"
        data_main_path = "./data_where_you_store/***.csv"
        
   
#### Running ABTest
    
    groups = "groups"
    test_groups = "test_groups"
    feature = "feature"
    data_source = "postgresql"
    connector = {"user": ***, "password": ***, "server": "127.0.0.1", 
    "port": ****, "db": ***}
    data_main_path ="""
                    SELECT                             
                    groups,
                    test_groups
                    feature,
                    time_indicator
                    FROM table
    """
    confidence_level = [0.01, 0.05]
    boostrap_ratio = [0.1, 0.2]
    export_path =  abspath("") + '/data'
    
    ab = ABTest(test_groups=test_groups, 
            groups=groups, 
            feature=feature, 
            data_source=data_source,
            data_query_path=query, 
            time_period=time_period, 
            time_indicator=time_indicator,
            time_schedule=time_schedule,
            export_path=export_path, 
            connector=connector, 
            confidence_level=confidence_level, 
            boostrap_sample_ratio=boostrap_ratio)
    ab.ab_test_init()
    
    
#### Schedule

Platform allows you to schedule your ABTest weekly, daily, monthly, hourly, every Monday, Tuesday, ..., Sunday.
    
***time_schedule :*** Additional to ABTest parameters, this parameter allows you to fix the time period.
-   daily schedule: Daily
-   monthly schedule: Monthly
-   day of week schedule: Monday - Mondays, Tuesday - Tuesdays, Wednesday - Wednesdays
-   hourly schedule: Hourly from ab_test_platform.executor import ABTest 

        groups = "groups"
        test_groups = "test_groups"
        feature = "feature"
        data_source = "postgresql"
        data_source = "postgresql"
        connector = {"user": ***, "password": ***, "server": "127.0.0.1", 
        "port": ****, "db": ***}
        data_main_path ="""
                            SELECT                             
                            groups,
                            test_groups
                            feature,
                            time_indicator
                            FROM table
        """
        confidence_level = [0.01, 0.05]
        boostrap_ratio = [0.1, 0.2]
        export_path =  abspath("") + '/data'
        
        ab = ABTest(test_groups=test_groups, 
                    groups=groups, 
                    feature=feature, 
                    data_source=data_source,
                    data_query_path=query, 
                    time_period=time_period, 
                    time_indicator=time_indicator,
                    time_schedule=time_schedule,
                    export_path=export_path, 
                    connector=connector, 
                    confidence_level=confidence_level, 
                    boostrap_sample_ratio=boostrap_ratio)
        ab.schedule_test()
                    
        
    Every 1 hour at 00:50:00 do run_ab_test() (last run: [never], next run: 2020-12-03 22:50:00)
    
    

Once you have assign the parameter time_schedule, A/B Test will be run with the recent date and recent date will be updated by ***time_period** and **time_schedule**. 

***e.g.*** **1st iteration:** recent date = 2020-12-05 00:00, time_schedule=Hourly. **2nd iteration:** recent date = 2020-12-05 01:00 (updated).

***e.g.*** **1st iteration:** recent date = 2020-12-05 00:00, start_date = 2020-11-29 00:00 (recent date - 1 week) time_schedule=Hourly. 
time_period=Weekly, **2nd iteration:** recent date = 2020-12-05 01:00 (updated)  start_date = 2020-11-29 01:00 (recent date - 1 week)
**This parameter must be assigned when A/B Test is scheduling**.
   

   


    

    