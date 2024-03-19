from pandas import Int64Dtype as Int64
import pandas as pd
import numpy as np
import pandera as pa
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Float, Boolean, DateTime, Text, ForeignKey, create_engine, text, Index
import pdb
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
import yaml
from sqlalchemy.exc import ProgrammingError
from tqdm import tqdm
import os

class NbaData_DB_Config:
    def __init__(self):
        self.base_directory = Path(__file__).parent.parent
        self.tables = [
            'tbl_game_detail',
            'tbl_player',
            'tbl_box'
        ]
        self.user_name = os.environ.get('PSQL_DB_USER')
        if not self.user_name:
            raise ValueError('PSQL_DB_USER environment variable is not set')
        self.DB_Name = 'nba_data'
        
        self.db_uri = f'postgresql://{self.user_name}@localhost:5432/'
        self.engine = create_engine(self.db_uri, isolation_level='AUTOCOMMIT')
        with self.engine.connect() as connection:
            connection.connection.commit()
            try:
                connection.execute(text(f'CREATE DATABASE {self.DB_Name}'))
            except ProgrammingError:
                print(f"The database {self.DB_Name} already exists.")
        self.engine = create_engine(f'postgresql://{self.user_name}@localhost:5432/{self.DB_Name}', echo=False)
        self.meta = MetaData()

    def read_table(self, data, schema, dataset_name):
        df = pd.read_sql(f'SELECT * FROM {schema}.{dataset_name}', self.engine)
        return df
    
    def read_table_with_query(self, query):
        df = pd.read_sql(query, self.engine)
        return df   

    def load_data(self, data, schema, dataset_name, progress_bar=True, new_data=False):
        if new_data == True:
            conn = self.engine.connect()
            conn.execute(text(f'TRUNCATE TABLE {schema}.{dataset_name}'))
            # Commit the transaction (optional, depending on your database system)
            conn.commit()
            conn.close()
        self.insert_with_progress(data, dataset_name, schema, progress_bar)

    def create_schema_if_not_exists(self, schema_name):
        # SQL statement to create schema if it doesn't exist
        create_schema_statement = text(f"CREATE SCHEMA IF NOT EXISTS \"{schema_name}\"")
        with self.engine.begin() as connection:
            connection.execute(create_schema_statement)

    def filter_and_set_dtypes(self, df, schema_name, table_name):
        table_info = self.get_table_info(schema_name, table_name)
        columns_to_keep = [col['name'] for col in table_info['columns']]
        # Step 1: Filter columns based on the dictionary keys
        filtered_df = df.filter(items=columns_to_keep)  
        # Step 2: Convert columns according to the dictionary
        for item in table_info['columns']:
            try:
                filtered_df[item['name']] = filtered_df[item['name']].astype(item['python_type']) 
            except Exception as e:
                print(e)
                print(item['name'])
                pdb.set_trace()
        filtered_df.replace('None', None, inplace=True)
        filtered_df.replace('', None, inplace=True)               
        filtered_df.columns = [x.lower() for x in filtered_df.columns]
        return filtered_df

    def create_table(self, schema_name, table_names=[]):
        self.create_schema_if_not_exists(schema_name)
        for table_name in table_names:
            self.get_table_schema(schema_name, table_name)
        self.meta.create_all(self.engine)
         
    def _map_sqlalchemy_type(self, type_name):
        type_mapping = {
            'Integer': Integer,
            'String': String,
            'Boolean': Boolean,
            'Date': Date,
            'DateTime': DateTime,
            'Float': Float,
            'Text': Text,
        }
        return type_mapping.get(type_name, String)  # Default to String if type_name is not found
    
    def get_table_info(self, schema_name, table_name):
        with open('nba_data_tbl_schema.yaml', 'r') as f:
            schema_definition = yaml.safe_load(f)
        return schema_definition[schema_name][table_name]

    def get_table_schema(self, schema_name, table_name):
        table_info = self.get_table_info(schema_name, table_name)
        columns = []
        for col in table_info['columns']:
            column = Column(col['name'],
                            self._map_sqlalchemy_type(col['sql_type']),
                            ForeignKey(col['foreign_key']) if 'foreign_key' in col else None,
                            primary_key=col.get('primary_key', False),
                            nullable=not col.get('not_null', False),
                            unique=col.get('unique', False))
            columns.append(column)
        
        table = Table(table_name, self.meta, *columns, schema=schema_name)  
        # if table_name == 'tbl_player':
        #     pdb.set_trace()
        if 'indexes' in table_info:
            for idx in table_info['indexes']:
                Index('idx_' + '_'.join(idx['columns']), 
                    *[getattr(table.c, col_name) for col_name in idx['columns']],
                    unique=idx.get('unique', False))
        return table


    def insert_with_progress(self, data, dataset_name, schema, progress_bar):
        def insert_on_conflict_nothing(table_to_insert, values_to_insert):
            conn = self.engine.connect()
            primary_key_columns = [key.name for key in table_to_insert.primary_key]
            try:
                stmt = insert(table_to_insert).values(values_to_insert).on_conflict_do_nothing(index_elements=primary_key_columns)
                result = conn.execute(stmt)
            except Exception as e:
                print(e)
                pdb.set_trace()
                return 0 
            finally:
                conn.commit()
                conn.close()
            return result.rowcount
            
        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        if progress_bar == False:
            values_to_insert = data.to_dict(orient='records')
            table_to_insert = Table(dataset_name,
                                    MetaData(),
                                    schema=schema.lower(),
                                    autoload_with=self.engine,
                                    extend_existing=True)
            insert_on_conflict_nothing(table_to_insert, values_to_insert)
        else:
            chunksize = 10_000

            # Set total as the length of the DataFrame
            total = len(data)

            with tqdm(total=total, desc="Inserting Data") as pbar:
                for _, cdf in enumerate(chunker(data, chunksize)):
                    values_to_insert = cdf.to_dict(orient='records')
                    table_to_insert = Table(dataset_name,
                                            MetaData(),
                                            schema=schema.lower(),
                                            autoload_with=self.engine,
                                            extend_existing=True)

                    insert_on_conflict_nothing(table_to_insert, values_to_insert)
                    # Update progress bar with the chunk size
                    pbar.update(len(cdf))


