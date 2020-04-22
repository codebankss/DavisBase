#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 18:10:47 2020

@author: hduser
"""

import os
import struct
import sys
from datetime import datetime, time
import sqlparse
import operator
import shlex
import re
import pdb
from operator import itemgetter


table_delete_recursion

def check_input(command):
    if len(command)==0:
        pass
    elif command[-1]!=";":
        return command

    elif command.lower() == "help;":
        help()

    elif command.lower() == "show tables;":
        show_tables()

    elif command[0:len("create table ")].lower() == "create table ":
        create_table(command)

    elif command[0:len("drop table ")].lower() == "drop table ":
        drop_table(command)

    elif command[0:len("create index ")].lower() == "create index ":
        create_index(command)

    elif command[0:len("insert ")].lower() == "insert ":
        insert_into(command)

    elif command[0:len("delete ")].lower() == "delete ":
        delete_from(command)

    elif command[0:len("update ")].lower() == "update ":
        update(command)

    elif command[0:len("select ")].lower() == "select ":
        table_name, cells = where(command)
        if table_name==None:
            return
        print_cells(table_name, cells)
        return None

    elif command.lower()=='test;':
        return 'break'

    elif command.lower() == "exit;":
        return True


    else:
        print("Command \"{}\" not recognized".format(command))







def help():
    print("DavisBase supported commands.")
    print("##########################################")
    print("SHOW TABLES;")
    print("CREATE TABLE ...;")
    print("DROP TABLE ...;")
    print("CREATE INDEX ...;")
    print("INSERT INTO ...;")
    print("DELETE FROM ...;")
    print("UPDATE ...")
    print("SELECT ...;")
    print("EXIT;")
    return None






# DDL
# show table, create table, drop table




# SHOW TabLES

def show_tables():
    """Go into the catalog,
    table_name = 'davisbase_tables'
    1. get all cells from davisbase_tables.tbl  get_all_table_cells(table_name)
    2.  Use print_cells (the function you wrote) """
    print_it("davisbase_tables.tbl", page_format=False, limit=None)
    return None



# CLI FUNCTIONS
"""NEEDS CONNECTING TO CREATE_TABLE_PARSER"""
def create_table(command):
    """Given the inputs of the command line, creates table, metadata, and indexes"""
    col_catalog_dictionary = parse_create_table(command)

    table_name = list(col_catalog_dictionary.keys())[0]
    if os.path.exists(table_name+'.tbl'):
        print("Table {} already exists.".format(table_name))
        return None
    initialize_file(table_name, True)
    catalog_add_table(col_catalog_dictionary)
    initialize_indexes(col_catalog_dictionary)
    return None



"""NEEDS CONNECTING TO PARSER"""
def drop_table(command):
    table_name = parse_drop_table(command)
    if os.path.exists(table_name+".tbl"):
        os.remove(table_name+".tbl")
        _, rows = schema_from_catalog(table_name, with_rowid=True)
        rowids = [row['rowid'] for row in rows]
        table_delete('davisbase_columns.tbl', rowids)
        data = read_all_pages_in_file('davisbase_tables.tbl')
        for page in data:
            if not page['is_leaf']:
                continue
            for cell in page['cells']:
                if table_name==cell['data'][0].lower():
                    rowids = [cell['rowid']]
                    break
        table_delete('davisbase_tables.tbl', rowids)
        for index in get_indexes(table_name.upper()):
            os.remove(index)
    else:
        print("Table \"{}\" does not exist.".format(table_name))







































# DEPENDENNCIES




# SHOW TABLE DEPENDENCIES

def print_it(file_name, page_format=False, limit=None, pages=None):
    """Used for testing, prints all contents of a table/index in page form or as list"""
    if pages ==None:
        pages  =read_all_pages_in_file(file_name)

    print(file_name[:-4].upper())
    if page_format:
        for page in pages:
            if page["is_leaf"]:
                continue
            else:
                print()
                print("page_number: ",page['page_number'])
                print("parent_page: ",page['parent_page'])
                print("right_child_page: ",page['rightmost_child_page'])
                print("bytes remaining:", page['available_bytes'])
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        print("rowid: ",cell['rowid'],"left child: ",cell['left_child_page'])
                    else:
                        print("indx_val: ",cell['index_value'],"left child: ",cell['left_child_page'])
        for page in pages:
            if not page["is_leaf"]:
                continue
            else:
                print()
                print("page_number: ",page['page_number'])
                print("parent_page: ",page['parent_page'])
                print("right_sibling_page: ",page['right_sibling_page'])
                print("bytes remaining:", page['available_bytes'])
                rowids = []
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        rowids.append(cell['rowid'])
                    else:
                        rowids.append(cell['index_value'])
                print(rowids)
    else:
        rows = []
        for page in pages:
            if not page["is_leaf"]:
                continue
            else:
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        rows.append([cell['rowid']]+cell['data'])
                    else:
                        rows.append([cell['index_value'],cell['assoc_rowids']])
        rows = sorted(rows, key=lambda x: x[0])
        i=1
        for row in rows:
            if limit!=None and i>limit:
                break
            print(row)
            i+=1







def read_all_pages_in_file(file_name):
    """
    Given the file name, loads all data from every page. This is what we will use during inserts updates, deletes

    Parameters:
    file_name (string): ex"davisbase_tables.tbl"

    Returns:
    pages (dict of dicts): ex. b'\x00\x00\x00\x00\x00\x00\x00\x00'

    """
    if file_name[-3:]=='tbl':
        is_table=True
    else:
        is_table = False

    file = load_file(file_name)
    file_size = len(file)
    assert(file_size%PAGE_SIZE==0)
    num_pages = int(file_size/PAGE_SIZE)
    data = []
    for page_num in range(num_pages):
        data.append(read_cells_in_page(file, page_num))
    for page in data:
        if page['is_leaf']:
            if page['right_sibling_page']!=-1:
                if data[page['right_sibling_page']]['parent_page']==page['parent_page']:
                    data[page['right_sibling_page']]['left_sibling_page'] = page['page_number']
        else:
            for i, cell in enumerate(page['cells']):
                child_page = cell['left_child_page']
                if i!=0:
                    data[child_page]['left_sibling_page']=page['cells'][i-1]['left_child_page']
                if i+1!=len(page['cells']):
                    data[child_page]['right_sibling_page']=page['cells'][i+1]['left_child_page']
                else:
                    data[child_page]['right_sibling_page']=page['rightmost_child_page']
            data[page['rightmost_child_page']]['left_sibling_page']=page['cells'][-1]['left_child_page']
            data[page['rightmost_child_page']]['right_sibling_page']=-1


    return data










def load_file(file_name):
    """loads the table/index file returns the bytestring for the entire file (reduce number of read/writes)

    Parameters:
    file (byte-string): ex 'taco.tbl'
    page_num (int): 1

    Returns:
    page (bytestring):
    """
    with open(file_name, 'rb') as f:
        return f.read()








def read_cells_in_page(file_bytes, page_num):
    """read all the data from a page, get the file_bytes object with load_file(file_name)"""
    assert(page_num<(len(file_bytes)/PAGE_SIZE))
    page = page_available_bytes(file_bytes, page_num)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    parent_page = struct.unpack(endian+'i', page[10:14])[0]
    available_bytes = page_available_bytes(file_bytes, page_num)
    if page[0] in [5,13]:
        is_table = True
    else:
        is_table = False

    if page[0] in [2,5]:
        is_interior = True
    else:
        is_interior = False

    i=0
    data = []
    while i<=num_cells-1:
        if i == 0:
            cell_bot_loc = PAGE_SIZE
        else:
            cell_bot_loc = struct.unpack(endian+'h',page[16+2*(i-1):16+2*(i)])[0]
        cell_top_loc = struct.unpack(endian+'h',page[16+2*i:16+2*(i+1)])[0]
        cell = page[cell_top_loc:cell_bot_loc]
        if is_table:
            data.append(table_read_cell(cell, is_interior))
        else:
            data.append(index_read_cell(cell, is_interior))
        i+=1

    result = {
    "page_number":page_num,
    "parent_page":parent_page,
    "is_table": is_table,
    "is_leaf": not is_interior,
    "num_cells":num_cells,
    "available_bytes":available_bytes
    }
    if is_interior:
        result['rightmost_child_page'] = parent_page = struct.unpack(endian+'i', page[6:10])[0]
    else:
        result['right_sibling_page'] = parent_page = struct.unpack(endian+'i', page[6:10])[0]
    result['cells']=data
    if is_table:
        result['rowids'] = [i['rowid'] for i in data]
    else:
        result['index_values'] = [i['index_value'] for i in data]
    return result




def load_page(file_bytes, page_num):
    """
    loads the page of from the table/index PAGE NUMBER STARTS AT ZERO, will only load one pa
    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    Returns:
    page (bytestring):
    """
    file_offset = page_num*PAGE_SIZE
    return file_bytes[file_offset:(page_num+1)*PAGE_SIZE]





def page_available_bytes(file_bytes, page_num):
    page = load_page(file_bytes, page_num)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    bytes_from_top = 16+(2*num_cells)
    cell_content_start =struct.unpack(endian+'h', page[4:6])[0]
    return  cell_content_start - bytes_from_top






def table_read_cell(cell, is_interior):
    """
    Used to read the contents of a cell (byte string)

    Parameters:
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'
    is_interior (bool):

    Returns:
    values (dictionary): ex.
    interior-> {'left_child_rowid': 1, 'rowid': 10, 'cell_size': 8}
    leaf ->{'bytes_in_payload': 61,'num_columns': 10,
            'data': [2, 2, 12,10,10, 1.2999999523162842, None,2020, None,10, 10,'hist'],
            'cell_size': 67}
    """
    is_leaf = not is_interior

    if  is_interior:
        cell_header = struct.unpack(endian+'ii', cell[0:8])
        res = {'left_child_page':cell_header[0],'rowid':cell_header[1]}
    elif is_leaf:
        cell_header = struct.unpack(endian+'hi', cell[0:6])
        payload = cell[6:]
        values = table_payload_to_values(payload)
        res = {'bytes':cell_header[0]+6, 'rowid':cell_header[1],"data":values}
    else:
        print("error in read cell")
    res["cell_size"]=len(cell)
    res['cell_binary'] = cell
    return res



def table_payload_to_values(payload):
    """
    Takes the entire bitstring payload and outputs the values in a list (None=Null)
    """
    num_columns = payload[0]
    temp = payload[1:]
    dtypes =  temp[:num_columns]
    temp = temp[num_columns:]
    i = 0
    values = []
    for dt in dtypes:
        element_size = get_dt_size(dt)
        byte_str = temp[i:i+element_size]
        values.append(dtype_byte_to_val(dt, byte_str))
        i+=element_size
    assert(i==len(temp))
    return values





def get_dt_size(dt):
    """given the single-digit encoding for data type return the number of bytes this data takes"""
    if dt==0:
        return 0
    if dt in [1,8]:
        return 1
    elif dt in [2]:
        return 2
    elif dt in [3,5,9]:
        return 4
    elif dt in [4,6,10,11]:
        return 8
    elif dt>=12:
        return dt-12
    else:
        raise ValueError("what happened????")





def dtype_byte_to_val(dt, byte_str):
    """Given the single-digit dtype encoding and byte string of approp size, returns Python value"""
    if dt==0:  #null type
        return None
    elif dt==1: #onebyteint
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)
    elif dt==8: #one byte year
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)+2000
    elif dt in [2,3,4,5,6]: #alldtypes i can convert with struct object
        return struct.unpack(int_to_fstring(dt), byte_str)[0]
    if dt in [10,11]: #datetime, dateobjects
        return bytes_to_dates(byte_str)
    if dt==9:#time
        return byte_to_time(byte_str)
    elif dt>=12:  #text
        return byte_str.decode("utf-8")
    else:
         raise ValueError("dtype_byte_to_val????")



def int_to_fstring(key):
    """format string for use in struct.pack/struct.unpack"""
    int2packstring={
    2:'h', 3:'i', 4:'q', 5:'f', 6:'d',
    9:'i', 10:'Q', 11:'Q' }
    return int2packstring[key]



def bytes_to_dates(bt, time=False):
    if not time:
        return datetime.fromtimestamp((struct.unpack(">q", bt)[0])/1000)
    else:
        return datetime.fromtimestamp((struct.unpack(">i", bt)[0])/1000)





def byte_to_time(bt):
    return bytes_to_dates(bt, time=True).time()



def index_read_cell(cell, is_interior):
    """
    Used to read the contents of a cell (byte string)

    Parameters:
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'
    is_interior (bool):

    Returns:
    values (dictionary):
    interior -> {'lchild': 12,'index_value': 1000,'assoc_rowids': [1, 2, 3, 4],'cell_size': 32}
    leaf-> {'index_value': 1000, 'assoc_rowids': [1, 2, 3, 4], 'cell_size': 28}
    """
    result=dict()
    if  is_interior:
        cell_header = struct.unpack(endian+'ih', cell[0:6])
        result["left_child_page"]=cell_header[0]
        result["bytes"]=cell_header[0]+6
        payload = cell[6:]
    else:
        cell_header = struct.unpack(endian+'h', cell[0:2])
        result["bytes"]=cell_header[0]+6
        payload = cell[2:]

    indx_value, rowid_list = index_payload_to_values(payload)
    result["index_value"]=indx_value
    result["assoc_rowids"]=rowid_list
    result["cell_size"]=len(cell)
    result['cell_binary'] = cell
    return result





def index_payload_to_values(payload):
    """import bytestring payload from index cell outputs the index value and list of rowids"""
    assoc_row_ids = payload[0]
    indx_dtype = payload[1]

    element_size = get_dt_size(indx_dtype)
    indx_byte_str = payload[2:2+element_size]
    indx_value = dtype_byte_to_val(indx_dtype, indx_byte_str)

    bin_rowid_list  = payload[2+element_size:]

    i=0
    j = len(bin_rowid_list)
    rowid_values = []
    while(i<j):
        rowid_values.append(struct.unpack(endian+'i', bin_rowid_list[i:i+4])[0])
        i+=4

    return indx_value, rowid_values




















































# CREATE TABLE DEPENDENCY





def parse_create_table(SQL):
    """
    Parses the raw, lower-cased input from the CLI controller. Will identify table name,
    column names, data types, and constraints. Will also check for syntax errors.
    Also check that table_name is all characters (no punctuation spaces...)

    Parameters:
    command (string):  lower-case string from CLI.
    (ex. "CREATE TABLE table_name (
             column_name1 data_type1 [NOT NULL][UNIQUE],
             column_name2 data_type2 [NOT NULL][UNIQUE],
            );""  )

    Returns:
    tuple: (table_name, column_list, definition_list)

    table_name: str
    column_list: list of column objects.

    SQL = \"""CREATE TABLE foo (
         id integer primary key,
         title varchar(200) not null,
         description text);\"""
    """
    SQL = SQL.rstrip()
    parsed = sqlparse.parse(SQL)[0]
    table_name = str(parsed[4])
    _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
    columns = extract_definitions(par)
    col_list = []
    definition_list = []
    for column in columns:
        definitions = ''.join(str(t) for t in column).split(',')
        for definition in definitions:
            d = ' '.join(str(t) for t in definition.split())
            col_list.append(definition.split()[0])
            definition_list.append(d)

    d = {}
    d[table_name] = {}
    c = 1
    for col, definition in zip(col_list, definition_list):
        isnull = 'NO'
        isunique = 'NO'
        isprimary = 'NO'
        definition = definition[len(col)+1:]
        if 'NOT NULL' in definition:
            isnull = 'YES'
        elif 'UNIQUE' in definition:
            isunique = 'YES'
        elif 'PRIMARY KEY' in definition:
            isprimary = 'YES'
            isunique = 'YES'
            isnull = 'YES'
        d[table_name][col] = {"data_type" : definition.split()[0],
                              "ordinal_position" : c,
                               'is_nullable':isnull,
                                'unique':isunique,
                                'primary_key':isprimary}
    return d





def extract_definitions(token_list):
    '''
    Subordinate function for create table to get column names and their definitions
    '''
    # assumes that token_list is a parenthesis
    definitions = []
    tmp = []
    # grab the first token, ignoring whitespace. idx=1 to skip open (
    tidx, token = token_list.token_next(1)
    while token and not token.match(sqlparse.tokens.Punctuation, ')'):
        tmp.append(token)
        # grab the next token, this times including whitespace
        tidx, token = token_list.token_next(tidx, skip_ws=False)
        # split on ",", except when on end of statement
        if token and token.match(sqlparse.tokens.Punctuation, ','):
            definitions.append(tmp)
            tmp = []
            tidx, token = token_list.token_next(tidx)
    if tmp and isinstance(tmp[0], sqlparse.sql.Identifier):
        definitions.append(tmp)
    return definitions





def initialize_file(table_name, is_table, is_interior=False, rchild=0):
    """Creates a file and writes the first, empty page (root)"""
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'
    if os.path.exists(table_name+file_type):
        os.remove(table_name+file_type)
    with open(table_name+file_type, 'w+') as f:
        pass
    write_new_page(table_name, is_table, is_interior, rchild, -1)
    return None



def write_new_page(table_name, is_table, is_interior, rsibling_rchild, parent):
    """Writes a empty page to the end of the file with an appropriate header for the kind of table/index"""
    assert(type(is_table)==bool)
    assert(type(is_interior)==bool)
    assert(type(rsibling_rchild)==int)
    assert(type(parent)==int)
    is_leaf = not is_interior
    is_index = not is_table
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'

    file_size = os.path.getsize(table_name + file_type)
    with open(table_name + file_type, 'ab') as f:
        newpage = bytearray(PAGE_SIZE*b'\x00')
        #first byte says what kind of page it is
        if is_table and is_interior:
            newpage[0:1] = b'\x05'
        elif is_table and is_leaf:
            newpage[0:1] = b'\x0d'
        elif is_index and is_interior:
            newpage[0:1] = b'\x02'
        elif is_index and is_leaf:
            newpage[0:1] = b'\x0a'
        else:
             raise ValueError("Page must be table/index")
        newpage[2:16] = struct.pack(endian+'hhii2x', 0, PAGE_SIZE, rsibling_rchild, parent)
        f.write(newpage)
        assert(file_size%PAGE_SIZE==0)
        return int(file_size/PAGE_SIZE)



def catalog_add_table(column_dictionary):
    """
    dictionary = {
    'table_name':{
        "column1":{
            'data_type':"int",
            'ordinal_position':1,
            'is_nullable':'YES',
            'unique':'NO'
            'primary_key':'YES'
            }
        }
    }
    """
    table = list(column_dictionary.keys())
    assert(len(table)==1)
    table_name = table[0].lower()
    columns =  column_dictionary[table_name.upper()]
    column_names = list(columns.keys())
    table_insert("davisbase_tables", [table_name])
    table_insert("davisbase_columns",[table_name, "rowid", "INT", 1, "NO", 'NO', 'NO' ] )
    for col in column_names:
        values=[table_name, col.lower(), columns[col]['data_type'].upper(), columns[col]['ordinal_position']+1, columns[col]['is_nullable'].upper(), columns[col]['unique'].upper(), columns[col]['primary_key'].upper()]
        table_insert("davisbase_columns", values)



def table_insert(table_name, values):
    """values would be a list of length self.columns, NULL represented as None"""
    schema, all_col_data = schema_from_catalog(table_name)
    next_page, next_rowid = get_next_page_rowid(table_name)
    cell = table_create_cell(schema, values, False,  rowid=next_rowid)
    try:
        page_insert_cell(table_name+'.tbl', next_page, cell)
    except:
        table_leaf_split_page(table_name+'.tbl', next_page, cell)
    return None




def schema_from_catalog(table_name, with_rowid=False):
    """Returns the column datatypes and a list of cells from davisbase_tables"""
    data = read_all_pages_in_file('davisbase_columns.tbl')
    all_cells = []
    all_data = []
    for page in data:
        if not page['is_leaf']:
            continue
        for cell in page['cells']:
            col_table = cell['data'][0].lower()
            if col_table==table_name.lower():
                col_name = cell['data'][1].lower()
                if col_name=='rowid' and not with_rowid:
                    continue
                all_cells.append((cell['data'][3],cell['data'][2])) #list of [(ord_pos, dtype)]
                all_data.append(cell)
    all_cells = sorted(all_cells, key=lambda x: x[0])
    schema = [i[1] for i in all_cells]
    return schema, all_data


def get_next_page_rowid(table_name):
    """Finds the next rowid and page for an insert"""
    pages = read_all_pages_in_file(table_name+'.tbl')
    final_page_num = 0
    while not pages[final_page_num]['is_leaf']:
        final_page_num = pages[final_page_num]['rightmost_child_page']

    final_page = pages[final_page_num]
    if len(pages[0]['cells'])==0:#if there are no records in the table
        next_rowid=0
    else:
        rowid_sorted_cells = sorted(final_page['cells'], key=lambda x: x['rowid'])
        next_rowid = rowid_sorted_cells[-1]['rowid']
    return final_page['page_number'], next_rowid + 1



def table_create_cell(schema, value_list, is_interior, left_child_page=None,  rowid=None):
    """
    Used to create a cell (binary string representation) that can be inserted into the tbl file

    Parameters:
    schema (list of strings):  ex. ['int', 'date', 'year']
    value_list (list of python values):  ex. [10, '2016-03-23_00:00:00',2004]
    is_interior (bool):  is the cell igoing into an interior or leaf page
    left_child_page (int):  page_no of left child (only if cell is in interior page).
    rowid (int):  rowid of the current cell (only if the cell is going in a leaf page)

    Returns:
    cell (byte-string): ex. b'\x00\x00\x00\x00\x00\x00\x00\x00'
    """
    assert(len(value_list)==len(schema))
    assert(type(schema)==list)
    assert(type(value_list)==list)
    assert(type(is_interior)==bool)

    if  is_interior:
        assert(left_child_page != None)
        assert(rowid != None)
        cell = struct.pack(endian+'ii', left_child_page, rowid)

    else:
        assert(rowid != None)
        payload_body, dtypes  = table_values_to_payload(schema, value_list)
        payload_header = bytes([len(dtypes)]) + bytes(dtypes)
        cell_payload = payload_header + payload_body
        cell_header = struct.pack(endian+'hi', len(cell_payload), rowid)
        cell = cell_header + cell_payload

    return cell




def table_values_to_payload(schema, value_list):
    """given a list of database string formatted datatypes ['int'] and an assoc
    list of values with NULL=None

    returns a bytestring of all elements in value_list and a single-digit repr of the data types"""
    dtypes = schema_to_int(schema, value_list)
    byte_string = b''
    for val, dt in zip(value_list, dtypes):
        byte_val = val_dtype_to_byte(val, dt)

        byte_string += byte_val
    return byte_string, dtypes




def schema_to_int(schema, values):
    """given a list of data types ex [int, year] ,convert to single-digit integer appropriate."""
    dtypes = [dtype_to_int(dt) for dt in schema]
    for i, val in enumerate(values):
        if val==None: #regardless of the col dtype, if null-> dt = 0
            dtypes[i]=0
            continue
        elif dtypes[i]==12: #add the len of the string to dtype
            dtypes[i]+=len(val)
    return dtypes




def dtype_to_int(dtype):
    """based on the documentation, each dtype has a single-digit integer encoding"""
    dtype = dtype.lower()
    mapping = {"null":0,"tinyint":1, "smallint":2, "int":3, "bigint":4, "long":4, 'float':5, "double":6, "year":8, "time":9, "datetime":10, "date":11, "text":12}
    return mapping[dtype]



def val_dtype_to_byte(val, dt):
    """given a value and a single-digit dtype rep, covert to binary string"""
    if val == None: #NULL
        return b''
    if dt==1: #one byte int
        return val.to_bytes(1, byteorder=sys.byteorder, signed=True)
    if dt==8: #one byte year relative to 2000
        return (val-2000).to_bytes(1, byteorder=sys.byteorder, signed=True)
    if dt in [2,3,4,5,6]: #alldtypes i can convert with struct object
        return struct.pack(int_to_fstring(dt), val)
    if dt in [10,11]: #datetime, date objects
        return date_to_bytes(val)
    if dt==9: #time object
        return time_to_byte(val)
    elif dt>=12:  #look for text
        return val.encode('ascii')



def date_to_bytes(date, time=False):
    if not time:
        return struct.pack(">q", int(round(date.timestamp() * 1000)))
    else:
        return struct.pack(">i", int(round(date.timestamp() * 1000)))


def time_to_byte(t):
    d =  datetime(1970,1,2,t.hour,t.minute, t.microsecond)
    return date_to_bytes(d, time=True)




def page_insert_cell(file_name, page_num, cell):
    """
    Inserts a bytestring into a page from a table or index file. Updates the page header. Fails if page-full

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    None
    """
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)

    if type(cell)==list:
        cells = cell
        for cell in cells:
            assert(len(cell)<page_available_bytes(file_bytes, page_num)) #CHECK IF PAGE FULL
            num_cells = struct.unpack(endian+'h', page[2:4])[0]
            bytes_from_top = 16+(2*num_cells)
            bytes_from_bot =struct.unpack(endian+'h', page[4:6])[0]
            new_start_index = bytes_from_bot - len(cell)
            #insert cell data
            page[new_start_index:bytes_from_bot] = cell
            #add to 2byte cell array
            page[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
            #update start of cell content
            page[4:6] = struct.pack(endian+'h', new_start_index)
            #update num_cells
            page[2:4] = struct.pack(endian+'h', num_cells+1)
            assert(len(page)==PAGE_SIZE)
    else:
        assert(len(cell)<page_available_bytes(file_bytes, page_num)) #CHECK IF PAGE FULL
        num_cells = struct.unpack(endian+'h', page[2:4])[0]
        bytes_from_top = 16+(2*num_cells)
        bytes_from_bot =struct.unpack(endian+'h', page[4:6])[0]
        new_start_index = bytes_from_bot - len(cell)
        #insert cell data
        page[new_start_index:bytes_from_bot] = cell
        #add to 2byte cell array
        page[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
        #update start of cell content
        page[4:6] = struct.pack(endian+'h', new_start_index)
        #update num_cells
        page[2:4] = struct.pack(endian+'h', num_cells+1)
        assert(len(page)==PAGE_SIZE)
    save_page(file_name, page_num, page)
    return None




def save_page(file_name, page_num, new_page_data):
    """
    Saves the overwrites the page in the file (at loc- page_num) with a byte-string of length PAGE_SIZE

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    new_page_data(bytestring): b'\r\x00\x07\x00\n\x01\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\xe0\x01\xc0\x01\xa4\x01\x80\x01\\\x013\x01\n\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    None
    """
    assert(len(new_page_data)==PAGE_SIZE)
    file_offset = page_num*PAGE_SIZE
    file_offset_end = (page_num+1)*PAGE_SIZE
    file_bytes = load_file(file_name)
    file_bytes = bytearray(file_bytes)
    file_bytes[file_offset:file_offset_end] = new_page_data
    with open(file_name, 'r+b') as f:
        f.seek(0)
        page = f.write(file_bytes)
    return None




def table_leaf_split_page(file_name, split_page_num, cell2insert):
    file_bytes = load_file(file_name)
    values = read_cells_in_page(file_bytes, split_page_num)

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_leaf = values['is_leaf']
    is_table = values['is_table']
    assert(is_table)
    assert(is_leaf)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)/2) #have to add one since we havent actually added the cell
    middle_cell_binary = cells[middle_cell]['cell_binary']
    middle_rowid = cells[middle_cell]['rowid']
    right_sibling_page = values['right_sibling_page']

    if parent_num==-1: #IS ROOT ->create two children
        rchild_num = write_new_page(table_name, is_table, False, -1, split_page_num)
        lchild_num = write_new_page(table_name, is_table, False, rchild_num, split_page_num)

        cells2copy = []
        for i in range(middle_cell):   #Copy cells into left child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, lchild_num, cells2copy)

        cells2copy = []
        for i in range(middle_cell, num_cells): #Copy cells into right child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, rchild_num, cells2copy)
        page_insert_cell(file_name, rchild_num, cell2insert)

        middle_cell_binary = table_create_cell([], [], True, left_child_page=lchild_num,  rowid=middle_rowid)
        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num, is_interior=True)


    else: #Non-root ->propagate upward
        rsibling = write_new_page(table_name, is_table, is_interior, right_sibling_page, parent_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rsibling)

        cells2copy = []
        for i in range(middle_cell, num_cells): #Copy cells into right child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, rsibling, cells2copy)
        page_insert_cell(file_name, rsibling, cell2insert)

        page_delete_cells_on_and_after(file_name, split_page_num, middle_cell)

        update_page_header(file_name, parent_num, rsibling_rchild=rsibling)
        middle_cell_binary = table_create_cell([], [], True, left_child_page=split_page_num,  rowid=middle_rowid)
        try:
            page_insert_cell(file_name, parent_num, middle_cell_binary)
        except:
            new_parent = table_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling)
            update_page_header(file_name,rsibling, parent = new_parent)
            update_page_header(file_name, split_page_num, parent = new_parent)



def page_delete_cells_on_and_after(file_name, page_num, cell_indx):
    """Deletes all cells in page on or after cell_indx (starts w zero)"""
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells>=1) #delete CAN empty a page
    assert(cell_indx>=0)
    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    cell_top_loc, cell_bot_loc = get_cell_indices(page, cell_indx)
    dis = cell_bot_loc - cell_content_area_start
    page[cell_content_area_start:cell_bot_loc] = b'\x00'*dis
    page[16+2*cell_indx:16+2*num_cells] = b'\x00'*2*(num_cells-cell_indx)
    #update num of cells
    page[2:4] = struct.pack(endian+'h', cell_indx)
    #change cell start area
    page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis)
    save_page(file_name, page_num, page)
    assert(len(page)==PAGE_SIZE) #ensure page is same size
    return (num_cells - 1) == 0

def get_cell_indices(page, cell_indx):
    cell_top_idx = struct.unpack(endian+'h',page[16+2*cell_indx:16+2*(cell_indx+1)])[0]
    if cell_indx==0: #if cell is first on the page (bottom)
        cell_bot_idx = PAGE_SIZE
    else:
        cell_bot_idx = struct.unpack(endian+'h',page[16+2*(cell_indx-1):16+2*(cell_indx)])[0]
    return cell_top_idx, cell_bot_idx




def update_page_header(file_name, page_num, rsibling_rchild=None, is_interior=None, parent=None):
    is_table = file_name[-4:]=='.tbl'
    is_index=not is_table
    is_leaf = not is_interior
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    if rsibling_rchild is not None:
        assert(len(file_bytes)/PAGE_SIZE>=rsibling_rchild)
        page[6:10] = struct.pack(endian+'i', rsibling_rchild)
    if is_interior is not None:
        if page[0] in [5,13]:
            is_table = True
        else:
            is_table = False

        if is_table and is_interior:
            page[0:1] = b'\x05'
        elif is_table and is_leaf:
            page[0:1] = b'\x0d'
        elif is_index and is_interior:
            page[0:1] = b'\x02'
        elif is_index and is_leaf:
            page[0:1] = b'\x0a'
    if parent is not None:
        page[10:14] = struct.pack(endian+'i', parent)
    save_page(file_name, page_num, page)
    return None



def table_interior_split_page(file_name, split_page_num, cell2insert, new_rightmost_page):
    pages = read_all_pages_in_file(file_name)
    values = pages[split_page_num]

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_table = values['is_table']
    assert(is_table)
    assert(is_interior)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)//2) #have to add one since we havent actually added the cell


    middle_cell_binary = cells[middle_cell]['cell_binary']
    middle_rowid = cells[middle_cell]['rowid']

    rightmost_child_page_right = new_rightmost_page
    rightmost_child_page_left = cells[middle_cell]['left_child_page']



    if parent_num==-1: #ROOT CONDITION #children will also be interior nodes
        rchild_num = write_new_page(table_name, is_table, is_interior, new_rightmost_page, split_page_num)
        lchild_num = write_new_page(table_name, is_table, is_interior, rightmost_child_page_left, split_page_num)

        cells2copy=[]
        for i in range(middle_cell):
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=lchild_num)
        update_page_header(file_name, rightmost_child_page_left, parent=lchild_num) #update parent of rightmost
        #Copy cells into left child
        page_insert_cell(file_name, lchild_num, cells2copy)

        cells2copy=[]
        for i in range(middle_cell+1, num_cells):#update child to point header to rchild
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=rchild_num)
        update_page_header(file_name, rightmost_child_page_right, parent=rchild_num)
        #Copy cells into right child
        page_insert_cell(file_name, rchild_num, cells2copy)
        page_insert_cell(file_name, rchild_num, cell2insert)
        #Update the pointers in the new, root node, then delete all but middle cell

        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_cell_lpointer(file_name, split_page_num, 0, lchild_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num)
        return rchild_num  #return so we can update headers of pages that couldnt fit in the old page

    else:
        rsibling = write_new_page(table_name, is_table, is_interior, rightmost_child_page_right, parent_num)

        cells2copy=[]
        for i in range(middle_cell+1, num_cells): #Copy cells into right child update child pointers
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=rsibling)
        update_page_header(file_name, rightmost_child_page_right, parent=rsibling)

        page_insert_cell(file_name, rsibling, cells2copy)
        page_insert_cell(file_name, rsibling, cell2insert)
        page_delete_cells_on_and_after(file_name, split_page_num, middle_cell)

        middle_cell_binary = table_create_cell([], [], True, left_child_page=split_page_num,  rowid=middle_rowid)
        update_page_header(file_name, split_page_num, rsibling_rchild=rightmost_child_page_left)

        if pages[parent_num]['rightmost_child_page']==split_page_num:
            update_page_header(file_name, parent_num, rsibling_rchild=rsibling)
        try:
            page_insert_cell(file_name, parent_num, middle_cell_binary)
        except:
            new_parent = table_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling)
            update_page_header(file_name, rsibling, parent = new_parent)
            update_page_header(file_name, split_page_num, parent = new_parent)
        return rsibling




def update_cell_lpointer(file_name, page_num, cell_indx, lpointer=None, rowid=None):
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    cell_top_idx, cell_bot_idx = get_cell_indices(page, cell_indx)
    if lpointer!=None:
        page[cell_top_idx:cell_top_idx+4] = struct.pack(endian+'i', lpointer)
    if rowid!=None:
        page[cell_top_idx+4:cell_top_idx+8] = struct.pack(endian+'i', rowid)
    save_page(file_name, page_num, page)
    return None




def initialize_indexes(column_dictionary):
    """
    dictionary = {
    'table_name':{
        "column1":{
            'data_type':"int",
            'ordinal_position':1,
            'is_nullable':'YES',
            'unique':'NO'
            'primary_key':'YES'
            }
        }
    }
    """
    table = list(column_dictionary.keys())
    table_name = table[0]
    column_names = list(column_dictionary[table_name].keys())
    columns = list(column_dictionary[table_name].values())

    for col in column_names:
        if column_dictionary[table_name][col]['primary_key']=='YES':
            index_name = table_name+'_'+col
            initialize_file(index_name, False) #create the empty ndx file for primary key
    return None




















































# DROP TABLE DEPENDENNCIES


def parse_drop_table(command):
    """
    Parses the raw, lower-cased input from the CLI controller. Will identify table name,
    Will also check for syntax errors. Throw error if

    Parameters:
    command (string):  lower-case string from CLI. (ex. "drop table table_name;"" )

    Returns:
    """
    ## check if the drop statement is correct or not
    ## statement must compulsarily end with semicolon

    return command[len("drop table "):-1].lower()
    #WRONG
    query_match = "(?i)DROP\s+(.*?)\s*(?i)TABLE\s+[a-zA-Z]+\;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        tablename = str(stmt.tokens[-2])
        return tablename
    else:
        print("Enter correct query")



def table_delete(file_name, rowids):
    pages = read_all_pages_in_file(file_name)
    for rowid in rowids:
        page_num=0
        table_delete_recursion(pages, page_num, rowid)
    page_dict_to_file(file_name, pages)





def table_delete_recursion(pages, page_num, rowid):
    page = pages[page_num]
    if page['is_leaf']:
        return delete_dict(pages, page_num, rowid) #returns 'left' pr 'right' or None
    else:
        for  i, cell in enumerate(page['cells']):
            if cell['rowid'] > rowid:
                child_page = cell['left_child_page']
                merge_child = table_delete_recursion(pages, child_page, rowid)
                break
            elif i+1 == len(page['cells']):
                child_page = page['rightmost_child_page']
                merge_child = table_delete_recursion(pages, child_page, rowid)
                break
            else:
                continue

        if merge_child is None: #all clear no merges necessary
            return None
        elif merge_child=='left': #merge left
            if page['parent_page']==-1 and page['num_cells']==1: #root condition
                merge_children(pages, page_num, child_page, left=True)
                pages[page['rightmost_child_page']]['parent_page'] = -1
                delete_page_in_dictionary(pages, page_num)
                #so ican find the root later
            else:
                id2del = merge_children(pages, page_num, child_page, left=True)
                return delete_dict(pages, page_num, id2del)

        elif merge_child=='right': #parent of a leaf node
            if page['parent_page']==-1 and page['num_cells']==1: #root condition
                merge_children(pages, page_num, child_page, left=False)
                pages[page['cells'][0]['left_child_page']]['parent_page'] = -1
                delete_page_in_dictionary(pages, page_num)
            else:
                id2del = merge_children(pages, page_num, child_page, left=False)
                return delete_dict(pages, page_num, id2del)
        else:
            assert(False)

































































