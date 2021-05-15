# DavisBase

## How to execute: 

The project is carried out in Python and follow the steps to execute the project:

1. Download the folder
2. In command prompt/terminal navigate to the project directory. Ex: $ cd ~/Desktop/project
3. Run the 'main' file using the following command: $ python main.py


NOTE:
'sqlparse' should be pre-installed, but, if executing the program gives out an error that 'sqlparse' is not installed, then run the following command in command prompt/terminal: $ pip install sqlparse
If it says 'requirement already satisfied' and the error persists, then you have multiple versions of python (in that case, create a virtual environment or install sqlparse for each of the version, like pip install and pip3 install).


## The project supports following commands:

1. CREATE TABLE
2. SHOW TABLE 
3. INSERT INTO TABLE
4. SELECT
5. SELECT-FROM-WHERE
6. DROP
7. EXIT


## Sample COMMANDS to run:


1. CREATE TABLE sample_table ( id int Primary key, sample_smallint smallint, sample_bigint bigint, sample_float float, sample_text text, sample_date date, sample_year year, sample_time time, sample_tinyint tinyint);

2. SHOW TABLES;

3. INSERT INTO sample_table (id, sample_smallint, sample_bigint, sample_float, sample_text, sample_date, sample_year, sample_time) VALUES (1, 10, 10101, 10.10, This is a text, 11/27/1996, 2020, NULL);

4. INSERT INTO sample_table (id, sample_smallint, sample_bigint, sample_float, sample_text, sample_date, sample_year, sample_time) VALUES (2, 20, 20202, 20.20, This is a second text, 11/27/1996, 2020, NULL);

5. SELECT * FROM sample_table;

6. SELECT * FROM sample_table WHERE id = 2;

7. EXIT;
