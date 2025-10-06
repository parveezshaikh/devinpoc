import json

class Component:
    """
    Component class to have the required attribute necessary for file loading
    """
    def __init__(self,id,component_name, component_type, input_data_file_type=None, input_data_file_path=None, input_schema_file_path=None,delimiter="", header=None,table_name=None,database_name=None,join_type=None,join_conditions=None,node_reference=None,sorting_type = None, sorting_column_name=None,output_data_file_type=None,output_data_file_path=None,filter_criteria=None,input_table_type=None,database_url=None,db_username=None,db_password=None,rollup_type=None,rollup_groupby_keys=None,rollup_aggregation=None,rollup_condition=None,dedup_sort_type=None,dedup_sort_columns=None,scan_type=None,scan_partition_key=None,scan_order_by_column=None,scan_sum_column=None,lookup_type=None,attribute=None,lookup_select_key1=None,lookup_select_key2=None,lookup_join_select_columns=None,lookup_join_columns=None,lookup_select_key=None,merge_type=None,merge_key=None,partition_type=None,partition_key=None):
        self.id = id
        self.component_name = component_name
        self.component_type = component_type
        self.input_data_file_type = input_data_file_type
        self.input_data_file_path = input_data_file_path
        self.input_schema_file_path =input_schema_file_path
        self.delimiter = delimiter
        self.header = header
        self.table_name =table_name
        self.database_name = database_name
        self.join_type =join_type
        self.join_conditions =join_conditions
        self.node_reference  = node_reference
        self.sorting_type = sorting_type
        self.sorting_column_name = sorting_column_name
        self.output_data_file_type = output_data_file_type
        self.output_data_file_path = output_data_file_path
        self.filter_criteria = filter_criteria
        self.input_table_type = input_table_type
        self.database_url = database_url
        self.db_username = db_username
        self.db_password = db_password
        self.rollup_type = rollup_type
        self.rollup_groupby_keys = rollup_groupby_keys
        self.rollup_aggregation = rollup_aggregation
        self.rollup_condition = rollup_condition
        self.dedup_sort_type = dedup_sort_type
        self.dedup_sort_columns = dedup_sort_columns
        self.scan_type = scan_type
        self.scan_partition_key = scan_partition_key
        self.scan_order_by_column = scan_order_by_column
        self.scan_sum_column = scan_sum_column
        self.lookup_type = lookup_type
        self.lookup_select_key1 = lookup_select_key1
        self.lookup_select_key2 = lookup_select_key2
        self.lookup_join_select_columns = lookup_join_select_columns
        self.lookup_join_columns = lookup_join_columns
        self.lookup_select_key = lookup_select_key
        self.merge_type = merge_type
        self.merge_key = merge_key
        self.partition_type = partition_type
        self.partition_key = partition_key



    @classmethod
    def from_json(cls, json_string):
        json_dict = json.loads(json_string)
        return cls(**json_dict)



