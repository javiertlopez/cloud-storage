import boto3
from boto3.dynamodb.conditions import Key, Attr

class DynamoStorage():
    def __init__(self, table, sortKey=False, region='us-east-1'):
        super().__init__()
        self._dynamodb = boto3.resource('dynamodb', region_name=region)
        self._client = boto3.client('dynamodb', region_name=region)
        try:
            if sortKey:
                response = self._dynamodb.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'keyword',
                            'AttributeType': 'S',
                        },
                        {
                            'AttributeName': 'inx',
                            'AttributeType': 'N',
                        },
                    ],
                    KeySchema=[
                        {
                            'AttributeName': 'keyword',
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': 'inx',
                            'KeyType': 'RANGE',
                        },
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5,
                    },
                    TableName=table,
                )
            else:
                response = self._dynamodb.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'keyword',
                            'AttributeType': 'S',
                        },
                    ],
                    KeySchema=[
                        {
                            'AttributeName': 'keyword',
                            'KeyType': 'HASH',
                        },
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5,
                    },
                    TableName=table,
                )
            response.meta.client.get_waiter('table_exists').wait(TableName=table)
        except Exception as e:
            pass

        self._table = self._dynamodb.Table(table)
        self._sortKey = sortKey
        print("dynamodb: "+table)

    def putAll(self, list):
        if len(list) == 0: return

        requests = []
        i = 0

        if self._sortKey:
            for key in list:
                requests.append(
                    {   'PutRequest': {
                            'Item': {
                                'keyword': {'S':key},
                                'inx': {'N': str(list[key][1])},
                                'value': {'S':list[key][0]}
                            }
                        }
                    }
                )
                i += 1
        else:
            for key in list:
                requests.append(
                    {   'PutRequest': {
                            'Item': {
                                'keyword': {'S':key},
                                'value': {'S':list[key][0]}
                            }
                        }
                    }
                )
                i += 1 

        try:
            self._client.batch_write_item(
                RequestItems={
                    self._table.name: requests
                }
            )
            print(f'dynamodb: batch_write_item {self._table.name}.')
        except Exception as e:
            print(f'dynamodb: batch_write_item {self._table.name}. Error: {e}')

    def getAll(self, search):
        result = []
        for item in search:
            response = self._table.query(
                KeyConditionExpression=Key('keyword').eq(item)
                )
            result = result + response['Items']

        response = []
        for item in result:
            response.append(item['value'])

        return response
