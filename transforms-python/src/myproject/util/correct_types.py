
import datetime
from numpy import datetime64
import os
import pandas as pd
import re
import time
import numpy as np

from prototype_2 import ddl
from ..util.ds_schema import domain_dataset_schema
from prototype_2.ddl import config_to_domain_name_dict
from prototype_2.ddl import domain_name_to_table_name
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types


def correct_record_list(record_list, column_name, intended_column_type, domain_name):
    for record in record_list:
        field_value = record[column_name]

        # FROM STRINGS
        if type(field_value) is str:
            if intended_column_type is int:
                record[column_name] = int(field_value)
                if type(record[column_name]) is not intended_column_type:
                    msg=f"CAST to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)

            if intended_column_type == np.int64:
                record[column_name] = np.int64(field_value)
                if type(record[column_name]) is not np.int64:
                    msg=f"CAST for int64 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)

            if intended_column_type == np.int32:
                intermediate_int32 = np.int32(field_value) # hopes of forcing an int, not a long
                #record[column_name] = int(intermediate_int32)
                record[column_name] = intermediate_int32
                if type(record[column_name]) is not np.int32:
                    msg=f"CAST for int32 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)

        # FROM various INTs, just pass an int, not int32 or int64, but go via int32 when that is asked for
        elif type(field_value) in (int, np.int32, np.int64):
            if intended_column_type is int:
                intermediate = np.int32(field_value)
                record[column_name] = int(intermediate)
                if type(record[column_name]) is not int:
                    msg=f"CAST for int to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            elif intended_column_type == np.int32:
                record[column_name] = np.int32(field_value) # hopes of forcing an int, not a long
                if type(record[column_name]) is not np.int32:
                    msg=f"CAST for int32 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            elif intended_column_type == np.int64:
                record[column_name] = np.int64(field_value)
                if type(record[column_name]) is not np.int64:
                    msg=f"CAST for int64 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            else:
                if type(record[column_name]) is not int:
                    msg=f"something FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)

        # FROM float
        elif type(field_value) in (np.float32, float, np.float64):
            if intended_column_type is float:
                record[column_name] = float(field_value)
                if type(record[column_name]) is not float:
                    msg=f"CAST for float to float FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            elif intended_column_type == np.float32:
                record[column_name] = np.float32(field_value)
                if type(record[column_name]) is not np.float32:
                    msg=f"CAST for float32 to float FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            elif intended_column_type == np.float64:
                record[column_name] = np.float64(field_value)
                if type(record[column_name]) is not np.float64:
                    msg=f"CAST for 64 to int FAILED: was:{type(field_value)}  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)
            else:
                if type(record[column_name]) is not float:
                    msg=f"something float FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                    raise Exception(msg)

def check_record_list(record_list, column_name, intended_column_type, domain_name):
    # Check and throw/raise if incorrect
    for record in record_list:
        # ASSUME NONE IS OK
        field_value = record[column_name]
        if intended_column_type is not np.int64 and \
           intended_column_type is not np.int32 and \
           intended_column_type is not np.float64 and \
           intended_column_type is not np.float32:
            if field_value is not None and type(field_value) is not  intended_column_type:
                msg=f"data and intended types don't agree cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                raise Exception(msg)
        elif intended_column_type == np.int64:
            if field_value is not None and type(field_value) is not np.int64:
                msg=f"intended type is np.int64 {intended_column_type} and we got something other than np.int64 {type(field_value)} cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                raise Exception(msg)
        elif intended_column_type == np.int32:
            if field_value is not None and type(field_value) is not np.int32:
                msg=f"intended type is  np.int32 and we got something other than np.int32 cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                raise Exception(msg)
        elif intended_column_type == np.float64:
            if field_value is not None and type(field_value) is not np.float64:
                msg=f"intended type is np.float64  and we got something other than np.float64 cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                raise Exception(msg)
        elif intended_column_type == np.float32:
            if field_value is not None and type(field_value) is not np.float32:
                msg=f"intended type is  np.float32 and we got something other than np.float32 cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                raise Exception(msg)
        
        # REGARDLESS
        # this is a problem?
        # field_value = record[column_name]
        # if type(field_value) is np.int64:
        #    msg=f"type int64 slipped through cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
        #    raise Exception(msg)
        # this is a problem?
        #if type(field_value) is np.int32:
        #    msg=f"type int32 slipped through cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
        #    raise Exception(msg)

def correct_types_in_record_list(domain_name, record_list):
    table_name = domain_name_to_table_name[domain_name]

    if table_name in domain_dataframe_column_types.keys():
        
        for column_name, intended_column_type in domain_dataframe_column_types[table_name].items():
            correct_record_list(record_list, column_name, intended_column_type, domain_name)
            check_record_list(record_list, column_name, intended_column_type, domain_name)
    
    else:
        msg=f"no bueno {domain_name} {domain_name} {table_name}"
        raise Exception(msg)


