from mysql_connecter import MySQL
from kafka import KafkaConsumer
import json
# topic, broker list 
def run() :
    mysql = MySQL(key_file='/opt/keys/aws_dc_sql_key.json',
                database='career-center')
    with open('./keys/dtype_map.json') as file :
        dtype_map = json.load(file)

    print('[INFO]Starting Kafka Consumer !')
    consumer = KafkaConsumer('flask_all_logs', 
                            bootstrap_servers=['52.78.62.228:9092'],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my-group',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            ) 
    print('run cunsumer')
    for message in consumer:
        # print('message :',message)
        data = message.value['Message']
        datetime = message.value['Asctime']
        if "activity" in data :
            table_name = data['activity']
            sql_dict = {
                    "user_id" : data['user_id'],
                    "datetime" : datetime.split(',')[0]
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

if __name__=='__main__':
    run()