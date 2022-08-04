from com.tmp.db.dynamodb import DynamoDB

kafka_cache: dict = {}


class BusinessLogic:
    cached_row: list = {}

    def get_kafka_cache(table_name, attribute_key, attribute_value):
        print('Get Kafka Cache:: attribute : ', attribute_key, ", attribute_value : ", attribute_value)
        if len(kafka_cache) != 0 and kafka_cache.get(attribute_value):
            cached_row = kafka_cache.get(attribute_value)
            print("Cached Record from Kafka Cache : ", cached_row)
        else:
            db = DynamoDB()
            cached_row = db.get_operation(table_name, attribute_key, attribute_value)

            print("Result of DB get_operation : ", cached_row, ", kafka_cache(before adding latest db record)  : ",
                  kafka_cache)

            kafka_cache[attribute_value] = cached_row
            print("kafka_cache (after adding DB record): ", kafka_cache)

        print("Complete Kafka Cache Records : ", kafka_cache, ", Final Cached Record : ", cached_row)
        print()

        return cached_row

    def populate_dynamodb(table_name, attribute_key, attribute_value, payload):
        print('Populate DynamoDB:: Table : ', table_name, ", attribute_key : ", attribute_key, ", attribute_value : ",
              attribute_value)

        db = DynamoDB()

        if db.table_exist(table_name):
            print("Table exist in db : ", table_name)
        else:
            print("Table not exist in db and creating... table : ", table_name)
            create_table_response = db.create_table_operation(table_name, attribute_key)
            print("create table response : ", create_table_response)

        load_table_response = db.post_operation(table_name, payload)
        print("load table response : ", load_table_response)

        record_loaded = db.get_operation(table_name, attribute_key, attribute_value)
        print("record_loaded : ", record_loaded)

        return record_loaded
