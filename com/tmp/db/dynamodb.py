import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')


class DynamoDB:

    @staticmethod
    def post_operation(table_name, payload):
        print('DynamoDB Post Operation :: Table_name :', table_name, ", Payload : ", payload)
        table = dynamodb.Table(table_name)
        table.put_item(Item=payload)
        print('DynamoDB Post Operation Successfully Performed :: ', payload)

        return 'success'

    @staticmethod
    def get_operation(table_name, attribute_key, attribute_value):
        print('DynamoDB GET Operation :: Table_name :', table_name, ", attribute_key : ", attribute_key,
              "attribute_value : ", attribute_value)

        table = dynamodb.Table(table_name)
        response = table.query(
            KeyConditionExpression=Key(attribute_key).eq(attribute_value)
        )

        items = response['Items']
        print('DynamoDB GET Operation Result :: Items :', items)

        return items

    # UpdateExpression='SET age = :val1',
    # ExpressionAttributeValues={':val1': 26}
    @staticmethod
    def update_operation(table_name, attribute_key, attribute_value, update_expression,
                         expression_attribute_values):
        table = dynamodb.Table(table_name)

        response = table.query(
            KeyConditionExpression=Key(attribute_key).eq(attribute_value)
        )
        items = response['Items']
        print(items)
        table.update_item(
            Key={
                attribute_key: attribute_value
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        return 'success'

    @staticmethod
    def delete_operation(table_name, attribute_key, attribute_value):
        table = dynamodb.Table(table_name)
        table.delete_item(
            Key={
                attribute_key: attribute_value
            }
        )
        print('performed delete operation')

    @staticmethod
    def table_exist(table_name):
        table_names = [table.name for table in dynamodb.tables.all()]

        if table_name in table_names:
            print('table : ', table_name, ' exists')
            return True
        else:
            print('table : ', table_name, ' Not exist')
            return False

    @staticmethod
    def create_table_operation(table_name, attribute_key):
        print('DynamoDB Create Table Operation :: Table_name :', table_name, ", attribute_key : ", attribute_key)
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': attribute_key,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': attribute_key,
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # Wait until the table exists.
        table.wait_until_exists()

        # Print out some data about the table
        print("DynamoDB Table Created with rows : ", table.item_count)

        return 'success'
