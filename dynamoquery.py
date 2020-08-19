import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

##SET VARIABLES##
aws_profile = input('AWS profile to use: ')
aws_region = input('AWS region to use: ')
db_table = input('DynamoDB table to query: ')
index_name = input('Index Name to use: ')
use_timestamp = input('Time bound query (Y/N): ').upper()
query_type = input('Query Type (Count or Matching Records): \nEnter 1 for Count, 2 for Matching Records')
filter_list = []

##SET BOTO3 SESSION VARIABLES##
session= boto3.session.Session(profile_name= aws_profile, region_name=aws_region)

def recordquery(match = None, customer = None, eventfilter1 = None, eventfilter2=None, start_date, end_date, dynamodb=None):
  if not dynamodb:
    dynamodb = session.resource('dynamodb', endpoint_url="https://dynamodb.{}.amazonaws.com").format(aws_region)
  table = dynamodb.Table(db_table)
  response = table.query(IndexName=index_name, Select={0},
    KeyConditionExpression = Key('Type').eq(match) & Key('Timestamp').between(start_date, end_date),
    FilterExpression = Attr('Role').contains(customer) & Attr('Event').ne(eventfilter1) & Attr('Event').ne(eventfilter2)
    )
  return response['Items']

def countquery(match = None, customer = None, eventfilter1 = None, eventfilter2=None, start_date, end_date, dynamodb=None):
  if not dynamodb:
    dynamodb = session.resource('dynamodb', endpoint_url="https://dynamodb.{}.amazonaws.com").format(aws_region)
  table = dynamodb.Table(db_table)
  response = table.query(IndexName=index_name, Select='COUNT',
    KeyConditionExpression = Key('Type').eq(match) & Key('Timestamp').between(start_date, end_date),
    FilterExpression = Attr('Role').contains(customer) & Attr('Event').ne(eventfilter1) & Attr('Event').ne(eventfilter2)
    )
  return response


if use_timestamp = 'Y':
  start_date = input('Enter start date (YYYY-MM-DD): ')
  end_date = input('Enter end date (YYYY-MM-DD): ')

def get_filters():
  global filter_list
  filter_query=input('Do you need to apply an additional filter to the results? (Y/N) ').upper()
  while filter_query = 'Y':
    filter_attribute = input('What is the name of the field you want to filter? ')
    filter_type = input("""Filter type? ('EQ'|'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'): """)
    filter_value = input('Value: ')
    filter_list.append((filter_attribute, filter_type, filter_value))
  

if query_type=='1':
  x=countquery()
else:
  x=recordquery()

print(x)
