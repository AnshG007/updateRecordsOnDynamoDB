#STEP-1
import boto3
from urllib.parse import urlparse, urlunparse
from pdf2image import convert_from_path
import requests
from datetime import datetime , timedelta
import math
from botocore.exceptions import NoCredentialsError
import os, shutil
import time
import fitz
import json
import sys
from botocore.exceptions import ClientError

log_dir = r""
os.makedirs(log_dir, exist_ok=True)

# Create a log file with date and time
log_file_name = f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file_path = os.path.join(log_dir, log_file_name)

# Redirect print statements to the log file
sys.stdout = open(log_file_path, 'w')
sys.stderr = sys.stdout

            
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = ''
meta_dict = {}
s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
S3_BUCKET_NAME = ''
pdf_file_path = r""
current_timestamp = time.time()

# print("Timestamp:", current_timestamp)
def yesterday_midnight_timestamp():
        
    current_unix_timestamp = int(time.time())
    seconds_since_midnight = current_unix_timestamp % 86400
    yesterday_midnight_unix_timestamp = current_unix_timestamp - seconds_since_midnight - 86400
    return yesterday_midnight_unix_timestamp


    
def current_midnight_timestamp():

    current_unix_timestamp = int(time.time())
    
    seconds_since_midnight = current_unix_timestamp % 86400
    
    today_midnight_unix_timestamp = current_unix_timestamp - seconds_since_midnight
    return today_midnight_unix_timestamp




def dynamo_db(table_name):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table(table_name)
    
    yesterday_timestamp = int(yesterday_midnight_timestamp())
    current_timestamp = int(current_midnight_timestamp())
    
    response = table.scan(
        FilterExpression='#t >= :start_timestamp AND #t <= :end_timestamp',
        ExpressionAttributeNames={
            '#t': 'timestamp'  
        },
        ExpressionAttributeValues={
            ':start_timestamp': yesterday_timestamp,
            ':end_timestamp': current_timestamp  
        }
    )
    #print(response["Items"])
    items = response.get('Items', [])
    s3url_org_doc_ids = []
    for item in items:
        s3url_org_doc_ids.append({
            #'user_id': item.get('userId')
            'org_id': item.get('orgId'),
            'annotation_key':item.get('annotationKey'),
            'user_id':item.get('userId'),
            'file_type':item.get('fileType'),
            'page_count':item.get('numberOfPages'),
            's3_url':item.get('s3Url'),
            'file_name':item.get('fileName')
        })
        
    return s3url_org_doc_ids


def signed_urls(url, expiration=3600):
    parsed_url = urlparse(url)
    #print(parsed_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    #print(bucket_name)
    object_key = parsed_url.path.lstrip('/')
    #print(object_key)
    s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Generate a pre-signed URL for the S3 object
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key,
        },
        ExpiresIn=expiration
    )
    print("")
    return presigned_url

def download_pdf_from_s3(sign_url , meta_dict):
    response = requests.get(sign_url)

    response.raise_for_status()
    pdf_name = meta_dict.get('file_name')
    print(pdf_name)
    try:
        os.makedirs(r"C:\Users\DELL\Desktop\dynamo_db_pdfs")
    except FileExistsError:
            pass
    local_filename = os.path.join(r"C:\Users\DELL\Desktop\dynamo_db_pdfs", f"{pdf_name}")
    
    with open(local_filename, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded file: {local_filename}, S3 URL: {sign_url}")
    
#STEP - 4
def pdf_page_count(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        page_count = doc.page_count
        doc.close()
        return page_count
    except Exception as e:
        print(f"Error while getting PDF page count: {e}")
        return None
        
def get_parent_json_data(url):
    parsed_url = urlparse(url)
    #print(parsed_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    #print(bucket_name)
    object_key = parsed_url.path.lstrip('/')
    #print(object_key)
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''
    AWS_REGION = ''
    
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    #s3_url = url
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    data_json = response['Body'].read()
  
    data = json.loads(data_json)
    return data 
    
def check_child_record(table_name , meta_dict):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table(table_name)
    
    response = table.scan(
        FilterExpression=' #o = :orgId AND #p = :parentId',
        ExpressionAttributeNames={
            '#o': 'orgId',
            '#p': 'parentId'  
        },
        ExpressionAttributeValues={
            ':orgId': f"{meta_dict.get('org_id')}",
            ':parentId': f"{meta_dict.get('annotation_key')}"
        }
    )
    #print(len(response['Items']))
    return len(response['Items'])
    

def convert_pdf_to_jpeg(page_count, meta_dict):
    pdf_name = meta_dict.get('file_name')
    pdf_path = os.path.join(r"", f"{pdf_name}")
    print(pdf_path)
    #page_count = int(meta_dict.get('page_count'))
   
    for page_number in range(1 , page_count + 1):
        pdf_name_without_extension = os.path.splitext(pdf_name)[0]
        images = convert_from_path(pdf_path)
        img_filename = f"{pdf_name_without_extension}_page{page_number}.jpeg"
        image_dir = os.path.join(r"", "images")
        try:
            os.makedirs(image_dir)
        except FileExistsError:
            pass
        print(image_dir)
        img_path = os.path.join(image_dir , img_filename)
    
        images[0].save(img_path, 'JPEG')
        

def dump_parent_json_into_children(url):
    parsed_url = urlparse(url)
    object_key = parsed_url.path.lstrip('/')
    object_key_without_extent = object_key[:-5]
    #print(object_key_without_extent)
    parent_json = get_parent_json_data(url)
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    for i, json_data in enumerate(parent_json):
        try:
            
            s3_json_object_key = f"{object_key_without_extent}_page{i+1}.json"
            
            json_object = json_data
            

            # Upload JSON object to S3
            response = s3.put_object(
                Body=json.dumps(json_object),
                Bucket='cf-ai-test2',
                Key=s3_json_object_key
            )
            json_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': 'cf-ai-test2', 'Key': s3_json_object_key},
                    ExpiresIn=5 * 365 * 24 * 60 * 60 
                )
            
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"JSON data uploaded successfully to S3: {json_url}")
            else:
                print(f"Failed to upload JSON data to S3: {json_url}. Status code: {response['ResponseMetadata']['HTTPStatusCode']}")
        except ClientError as e:
            print(f"An error occurred while uploading JSON data to S3: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def upload_jpeg_to_s3(meta_dict , bucket_name):
    jpeg_files = []
    jpeg_dict = []
    #page_count = int(meta_dict.get('page_count'))
    pdf_name = meta_dict.get('file_name')
    org_id = meta_dict.get('org_id')
    user_id = meta_dict.get('user_id')
    parent_id = meta_dict.get('annotation_key')
    pdf_name_without_extension = os.path.splitext(pdf_name)[0]
  
    image_path = os.path.join(r"" , "images")
    
    if not os.path.exists(image_path):
        os.makedirs(image_path)
    jpeg_files = os.listdir(image_path)
    #print(jpeg_files)
    for i , jpeg_file in enumerate(jpeg_files):
        s3_object_key = f"{str(org_id)}/{pdf_name}-{i+1}/{pdf_name_without_extension}_page{i+1}.jpeg"
        img_path = os.path.join(r"C:\Users\DELL\Desktop\dynamo_db_pdfs" , "images" , jpeg_file)
        s3.put_object(Body=open(img_path, 'rb'), Bucket=bucket_name, Key=s3_object_key)
        jpeg_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': s3_object_key},
                    ExpiresIn=5 * 365 * 24 * 60 * 60 
                )
        temp_jpeg_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_object_key}"
        #print(f"{jpeg_url} {temp_jpeg_url}")
        jpeg_dict.append({
            'org_id':org_id,
            'user_id':user_id,
            'parent_id':parent_id,
            'file_type':"jpeg",
            's3_url':jpeg_url,
            'file_name':jpeg_file,
            'page_count':i+1,
            'annotation_key':f"{org_id}-{jpeg_file}-{current_timestamp}"
        })
    return jpeg_dict
 

def update_children_to_dynamo_db(jpeg_dict):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table("AnnotationsInfo")
    for i , item in enumerate(jpeg_dict):
        try:
            response = table.put_item(
                Item={
                    'orgId': item.get('org_id'),
                    'userId': item.get('user_id'),
                    'parentId': item.get('parent_id'),
                    'fileName': item.get('file_name'),
                    'pageNumber': i+1,
                    's3Url': item.get('s3_url'),
                    'annotationKey':item.get('annotation_key'),
                    'documentType': item.get('file_type')
                }
            )
            print("Successfully data uploaded...............!!!!!! on Dynamo DB ")
        except Exception as e:
            print(f"Error : {e}")
            
            

def delete_folder():
    if os.path.isdir(r""):
        shutil.rmtree(r"")
    print("possibly remove all the files and folder of the specified path")

if __name__ == "__main__":
    print("Hello world")
    
    meta_dicts = dynamo_db('')
    print(meta_dicts)
    for meta_dict in meta_dicts:
        
        if meta_dict.get('file_name') == None:
            print(f"File Name is not present , In dynamodb file_name Field is None of annotation_key : {meta_dict.get('annotation_key')}")
        
        try:
            s3_url = meta_dict.get('s3_url')
            sign_url = signed_urls(s3_url)
        except:
            print(f"s3 URL of {meta_dict.get('file_name')} is not available")
        try:
            download_pdf_from_s3(sign_url , meta_dict)
        except:
        
            print(f"{meta_dict.get('file_name')} is not able to Download")
        
        try:
            pdf_path = os.path.join(pdf_file_path, f"{meta_dict.get('file_name')}")
            
            page_count = pdf_page_count(pdf_path)
            
            child_entries = check_child_record("" , meta_dict)
        

            if page_count == child_entries:
                print(f"Children of {meta_dict.get('file_name')} pdf exist already on Dynamodb")
            else:
                convert_pdf_to_jpeg(page_count , meta_dict)
        except Exception as e:
            print(f"Exception : {e}")
        
        jpeg_dict = ""        
        try:
            jpeg_dict = upload_jpeg_to_s3(meta_dict, S3_BUCKET_NAME)
        except:
            print(f"{meta_dict.get('file_name')} not able to upload on S3")
            
        try:
            url = meta_dict.get(json_url)
            dump_parent_json_into_children(url)
        except:
            print(f"Json_url of the file {meta_dict.get('file_name')} is not available")
        
        try:
            update_children_to_dynamo_db(jpeg_dict)
        except Exception as e:
            print(f"Exception : {e}")
            #print(f"Page count is not defined for this file {meta_dict.get('file_name')}")
            
    delete_folder()     

        
    sys.stdout.close()
    
    