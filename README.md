# WEb Scraping Project
## Problem Statement

An international firm that is looking to expand its business in different countries across the world wants to creat an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data is available on the URL mentioned below:

URL

1
'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

The required information needs to be made accessible as a `CSV` file `Countries_by_GDP.csv` as well as a table `Countries_by_GDP` in a database file `World_Economies.db` with attributes `Country` and `GDP_USD_billion`.

We will demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log in a file with the entire process of execution named etl_project_log.txt.

### Objectives
we have to complete the following tasks for this project

1. Write a data extraction function to retrieve the relevant information from the required URL.

2. Transform the available GDP information into 'Billion USD' from 'Million USD'.

3. Load the transformed information to the required CSV file and as a database file.

4. Run the required query on the database.

5. Log the progress of the code with appropriate timestamps.


## Initial setup
Before we start building the code, we need to install the required libraries for it.

The libraries needed for the code are as follows:

1. requests - The library used for accessing the information from the URL.

2. bs4 - The library containing the BeautifulSoup function used for webscraping.

3. pandas - The library used for processing the extracted data, storing it to required formats and communicating with the databases.

4. sqlite3 - The library required to create a database server connection.

5. numpy - The library required for the mathematical rounding operation as required in the objectives.

6. datetime - The library containing the function datetime used for extracting the timestamp for logging purposes.

## Preliminary: Importing libraries and defining known values

```python
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
```
We will import the URL into this Notebook

```python 
import requests

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
response = requests.get(url)

if response.status_code == 200:
    with open("source.zip", "wb") as file:
        file.write(response.content)
    print("File downloaded successfully.")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

```
out[ ]
```
File downloaded successfully.
```
**Further, we need to initialize all the known entities. These are mentioned below**:

**table_attribs**: The attributes or column names for the dataframe stored as a list. Since the data available in the website is in USD Millions, the attributes should initially be 'Country' and 'GDP_USD_millions'. This will be modified in the transform function later.

**db_name**: As mentioned in the Project scenario, 'World_Economies.db'

**table_name**: As mentioned in the Project scenario, 'Countries_by_GDP'

**csv_path**: As mentioned in the Project scenario, 'Countries_by_GDP.csv'

We will log the initialization process
The Table of interest in the web page is the image below
![GDP](https://github.com/nnannaeze/Web-Scraping-Project/assets/148848746/3a8e87c3-caf6-4e85-8c3a-c66c35f94f93)

## Task 1: Extracting information

Extraction of information from a web page is done using the web scraping process. For this, we'll have to analyze the link by going to the web page and coming up a strategy of how to get the required information. In our case we'll do the following.

1. Inspect the URL and note the position of the table. Note that even the images with captions in them are stored in tabular format. Hence, in the given webpage, our table is at the third position, or **index 2**. Among this, we require the entries under 'Country/Territory' and 'IMF -> Estimate'.

2. Note that there are a few entries in which the IMF estimate is shown to be '—'. Also, there is an entry at the top named 'World', which we do not require. Segregate this entry from the others because this entry does not have a hyperlink and all others in the table do. So you can take advantage of that and access only the rows for which the entry under 'Country/Terriroty' has a hyperlink associated with it.

Note that '—' is a special character and not a general hyphen, '-'. Copy the character from the instructions here to use in the code.

We'll create a function `extract()` the function gets the URL and the table_attribs parameters as arguments

#### Method
1. Extract the web page as text
by using this code: `page = requests.get(url).text`

2. Parse the text into an HTML object
this is done by the code : `data = BeautifulSoup(page,'html.parser')`

3. Create an empty pandas DataFrame named `df` with columns as the table_attribs
Using this code  `df = pd.DataFrame(columns=table_attribs)`
4. Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute.

   Using the code

   `tables = data.find_all('tbody')`
   
    `rows = tables[2].find_all('tr')`

5. Check the contents of each row, having attribute ‘td’, for the following conditions(`for row in rows:)
* The row should not be empty.(`if len(col)!=0:`)
* The first column should contain a hyperlink.(`if col[0].find('a') is not None`)
* The third column should not be '—'.(`and '—' not in col[2]:`)

6. Store all entries matching the conditions in step 5 to a dictionary(`data_dict`) with keys the same as entries of table_attribs`["Country", "GDP_USD_millions"]`. Append all these dictionaries one by one to the dataframe.
 
data_dict = {"Country": col[0].a.contents[0],`

             "GDP_USD_millions": col[2].contents[0]}

```python
def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
```
We will call up the Function `extract(url, table_attribs)` and use the `.head()`  to see the 1st 5 rows of the dataset to confirm the work

```python
extract(url, table_attribs).head()
```
out[ ]
![image](https://github.com/nnannaeze/Web-Scraping-Project/assets/148848746/ab5721d5-8fa8-4c9f-944b-6de21333e4ea)

## Task 2: Transform information
The transform function needs to modify the `‘GDP_USD_millions’`. We need to cover the following points as a part of the transformation process.

1. Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to a floating numbers using the following
    *  `tolist` to convert toa list
    *  `split(',')` to split the content of each cell by the delimeter (,) thus removing the (',') from the cell eg **:"26" "854" "599"**
    *  `join()`join the separated cell contents noting you re joing elements separated by "" therefore we use `"".join(x.split(','))`
    *  `float` convert to contents to float
2. Divide all these values by 1000 and round it to 2 decimal places
3. Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.

```python
ef transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df
```

Lets call-up the fuction `transform`

out[ ]
![image](https://github.com/nnannaeze/Web-Scraping-Project/assets/148848746/47ee9c7b-531e-4eed-ae65-a87f18584d06)

## Task 3: Loading information
Loading process for this project is two fold.

We'll have to save the transformed dataframe to a `CSV` file. For this, pass the dataframe df and the CSV file path to the function `load_to_csv()` and add the required statements there.

```python
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
```
We'll have to save the transformed dataframe as a table in the database. This needs to be implemented in the function `load_to_db()`, which accepts the dataframe df, the connection object to the SQL database conn, and the table name variable table_name to be used.

```python
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
```

## Task 4: Querying the database table
We'll create a function `run_query(query_statement, sql_connection)` so that when we initiate an appropriate querry statement and pass it to the function `run_query()`, along with the SQL connection object sql_connection and the table name variable `table_name`, this function(`run_query()`) should run the query statement on the table and retrieve the output as a filtered dataframe. This dataframe can then be simply printed.

```python
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
```
## Task 5: Logging progress
Logging needs to be done using the `log_progress()` funciton. This function will be called multiple times throughout the execution of this code and will be asked to add a log entry in a .txt file, etl_project_log.txt. The entry will be in the following format:

```python
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    print(timestamp + ' : ' + message)
```

## Function calls
Now, you have to set up the sequence of function calls for your assigned tasks. Follow the sequence below.

**Task**|	**Log message on completion**
Declaring known values |	Preliminaries complete. Initiating ETL process.

Call extract( ) | function	Data extraction complete. Initiating Transformation process.

Call transform( ) | function	Data transformation complete. Initiating loading process.

Call load_to_csv( )	| Data saved to CSV file.

Initiate SQLite3 connection	| SQL Connection initiated.

Call load_to_db( )	| Data loaded to Database as table. Running the query.

Call run_query( ) *	| Process Complete.

Close SQLite3 connection	

```python
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
```
out[ ]
```
2024-May-17-11:20:44 : Preliminaries complete. Initiating ETL process
2024-May-17-11:20:47 : Data extraction complete. Initiating Transformation process
2024-May-17-11:20:47 : Data transformation complete. Initiating loading process
2024-May-17-11:20:47 : Data saved to CSV file
2024-May-17-11:20:47 : SQL Connection initiated.
2024-May-17-11:20:48 : Data loaded to Database as table. Running the query
SELECT * from Countries_by_GDP WHERE GDP_USD_billions >= 100
          Country  GDP_USD_billions
0   United States          26854.60
1           China          19373.59
2           Japan           4409.74
3         Germany           4308.85
4           India           3736.88
..            ...               ...
64          Kenya            118.13
65         Angola            117.88
66           Oman            104.90
67      Guatemala            102.31
68       Bulgaria            100.64

[69 rows x 2 columns]
2024-May-17-11:20:48 : Process Complete.
```
## Conclusion

In this project, we performed complex Extract, Transform, and Loading operations on data from a webpage
We achieved the following

1. Extract relevant information from websites using Webscraping and requests API.
2. Transform the data to a required format.
3. Load the processed data to a local file or as a database table.
4. Query the database table using Python.
5. Create detailed logs of all operations conducted
