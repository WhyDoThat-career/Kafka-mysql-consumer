import pymysql

class MySQL :
    def __init__(self,key_file,database) :
        KEY = self.load_key(key_file)
        self.MYSQL_CONN = pymysql.connect(
            host = KEY['host'],
            user=KEY['user'],
            passwd=KEY['password'],
            db=database
            charset='utf8mb4'
        )
    def load_key(self,key_file) :
    with open(key_file) as key_file :
        key = json.load(key_file)
    return key

    def conn_mysqldb(self):
        if not MYSQL_CONN.open :
            MYSQL_CONN.ping(reconnect=True)
        return MYSQL_CONN
    
    def insert_data(self,table,items,dtype_map) :
        skey,sdata = self.dict_list2string(items,dtype_map)
        db = self.conn_mysqldb()
        db_cursor = db.cursor()
        sql_query = f"INSERT INTO {table} ({skey}) VALUES ({sdata})"
        db_cursor.execute(sql_query)
        db.commit()