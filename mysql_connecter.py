import pymysql
import json

class MySQL :
    def __init__(self,key_file,database) :
        KEY = self.load_key(key_file)
        self.MYSQL_CONN = pymysql.connect(
            host = KEY['host'],
            user=KEY['user'],
            passwd=KEY['password'],
            db=database,
            charset='utf8mb4'
        )
    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key

    def conn_mysqldb(self):
        if not self.MYSQL_CONN.open :
            self.MYSQL_CONN.ping(reconnect=True)
        return self.MYSQL_CONN

    def create_sql_item(self,item,dtype) :
        if dtype == 'string' or 'datetime':
            return "'{}'".format(item)
        elif dtype == 'bool' or 'int':
            return "{}".format(item)

    def arr2str(self,array) :
        if array is not None :
            return ','.join(array)
        else :
            return None

    def dict_list2string(self,items,dtype_map) :
        key_arr = []
        item_arr = []
        for key, item in items.items() :
            if key != 'id' and item != None :
                key_arr.append(key)
                item_arr.append(self.create_sql_item(item,dtype=dtype_map[key]))
        return self.arr2str(key_arr),self.arr2str(item_arr)
    
    def insert_data(self,table,items,dtype_map) :
        skey,sdata = self.dict_list2string(items,dtype_map)
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO {table} ({skey}) VALUES ({sdata})"
        db_cursor.execute(sql_query)
        db.commit()
        db.close()