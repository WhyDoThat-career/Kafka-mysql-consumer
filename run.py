from mysql_connecter import MySQL
from kafka import KafkaConsumer 
from json import loads 
# topic, broker list 
consumer = KafkaConsumer('flask_all_logs', 
                         bootstrap_servers=['52.78.62.228:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')),
                         consumer_timeout_ms=1000 ) 

mysql = MySQL(key_file='./keys/aws_dc_sql_key.json',
              database='career-center')
with open('./keys/dtype_map.json') as file :
    dtype_map = json.load(file)

for message in consumer :
    if message.key == 'analys' :
        data = message.value['Message']
        datetime = message['Asctime']
        table_name = data['activity']
        sql_dict = {
                "user_id" : data['user_id'],
                "datetime" : datetime
            }
        if table_name == 'resume_sector' :
            sql_dict['sector_id'] = data['resume_select']
        elif table_name == 'resume_skill' :
            sql_dict['skill_id'] = data['resume_select']
        elif table_name == 'filtering' :
            sql_dict['filtering'] = data['filter_text']
        else :
            sql_dict['recruit_id'] = data['recruit_id']

        mysql.insert_data(table_name,sql_dict,dtype_map)
        print(f"[SQL INPUT]Time : {datetime}, Key: {message.key}, Value: {data}")

        