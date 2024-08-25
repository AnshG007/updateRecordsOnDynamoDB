#STEP-1
import boto3
import pytz
import re
from urllib.parse import urlparse, urlunparse
from pdf2image import convert_from_path
import requests
from datetime import datetime as dt
import math
from botocore.exceptions import NoCredentialsError
import os, shutil
import time
import fitz
import json
import sys
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr , Key



#/home/ubuntu/creatingChildRecords/logs
#
log_dir = r"C:\Users\DELL\Desktop\logs_second"
os.makedirs(log_dir, exist_ok=True)

# Create a log file with date and time
log_file_name = f"output_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file_path = os.path.join(log_dir, log_file_name)

# Redirect print statements to the log file
sys.stdout = open(log_file_path, 'w')
sys.stderr = sys.stdout
#annotation_key = 'deepesh-081557189.pdf-202403161543'
#org_ID = 'deepesh'  
          
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = ''
meta_dict = {}
s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

pdf_file_path = r""
#/home/ubuntu/creatingChildRecords/dynamo_db_pdfs
#
current_timestamp = time.time()
# print("Timestamp:", current_timestamp)


def get_current_time():
    utc_time = datetime.datetime.utcnow()
    return utc_time.strftime('%I:%M:%S %p')


def get_current_date():
    return datetime.datetime.now().strftime('%-m/%-d/%Y') 

	
def get_current_timestamp():
    gmt_time = datetime.datetime.now(pytz.utc)
    return gmt_time.strftime('%Y%m%d%H%M')

def dynamo_db(table_name):
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        table = dynamodb.Table(table_name)
        # print(f"Current Timestamp : {current_timestamp}  Yesterday Timestamp : {yesterday_timestamp} , Yesterday Date : {yesterday}")
        
        all_items = []
        
        # Define the filter expression
        filter_expression = (
            Attr('annotationKey').eq("005Ho00000B4FIHIA3-081548885.pdf-202408071257")
        )

        # Perform the initial scan with the filter expression
        response = table.scan(
            FilterExpression=filter_expression
        )
        all_items.extend(response['Items'])
        # Continue to scan until all items are retrieved
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            all_items.extend(response['Items'])

        # Extract relevant information from items
        s3url_org_doc_ids = []
        for item in all_items:
            s3url_org_doc_ids.append({
                'org_id': item.get('orgId'),
                'annotation_key': item.get('annotationKey'),
                'user_id': item.get('userId'),
                'file_type': item.get('fileType'),
                'page_count': item.get('numberOfPages'),
                's3_url': item.get('s3Url'),
                'file_name': item.get('fileName'),
                'json_url': item.get('jsonUrl'),
                'bucket_name':item.get('bucketName'),
                'upload_date':item.get('uploadDate'),
                'uploaded_time':item.get('uploadedTime'),
                'document_type':item.get('documentType')
            })
        
        #print(s3url_org_doc_ids)    
        return s3url_org_doc_ids
    
    except Exception as e:
        print(f"dynamo db function error: {e}")
        return []

# Example call to the function (make sure to replace with actual table name and ensure variables like current_timestamp are defined)
# dynamo_db('YourTableName')



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
    #print("")
    return presigned_url

def download_pdf_from_s3(sign_url , meta_dict):
    response = requests.get(sign_url)

    response.raise_for_status()
    pdf_name = meta_dict.get('file_name')
    print("")
    print(pdf_name)
    try:
        os.makedirs(pdf_file_path)
    except FileExistsError:
            pass
    local_filename = os.path.join(pdf_file_path, f"{pdf_name}")
    
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
        
def get_parent_json_data(unsigned_url):
    url = signed_urls(unsigned_url)

    #print(f"in the function get_parent_json_data {url}")
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
    #print(response)
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
    pdf_path = os.path.join(pdf_file_path, f"{pdf_name}")
    #print(pdf_path)
    #page_count = int(meta_dict.get('page_count'))
   
    for page_number in range(1 , page_count + 1):
        pdf_name_without_extension = os.path.splitext(pdf_name)[0]
        images = convert_from_path(pdf_path, dpi=400)
        img_filename = f"{pdf_name_without_extension}-page{page_number}.jpeg"
        image_dir = os.path.join(pdf_file_path, "images")
        try:
            os.makedirs(image_dir)
        except FileExistsError:
            pass
        #print(f"image_dir :{image_dir}")
        img_path = os.path.join(image_dir , img_filename)
    
        images[page_number - 1].save(img_path, 'JPEG')
        

def dump_parent_json_into_children(meta_dict , url):
    #print("inside the function")
    #print(f"json_url : {url}")
    parsed_url = urlparse(url)
    bucket_name = parsed_url.netloc.split('.')[0]
    object_key = parsed_url.path.lstrip('/')
    filename = object_key.split('/')[-1]
    cleaned_filename = filename.replace(".pdf", "")
    cleaned_filename_without_extension = cleaned_filename[:-5]
    parts = object_key.split('/')

    # Remove the last part
    new_path = '/'.join(parts[:-1])
    #print(object_key_without_extent)
    
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    key_list = ["lineNumber","partNumber","description","quantityOrdered","quantityBackordered","quantityShipped","unitPrice","amount"]

    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    #print(response)
    data_json = response['Body'].read()
  
    parent_json = json.loads(data_json)
    #print(f"parent_json: {parent_json}")
    json_page_not_included = []
    json_urls = []
    for i, json_data in enumerate(parent_json):
        try:

            s3_json_object_key = f"{new_path}/{cleaned_filename_without_extension}-page{i+1}.json"
            #f"{object_key_without_extent}-page{i+1}.json"
            
            json_object = json_data
            def check_json_keys_not_empty(json_data, key_list):
                for data in json_data['items']:
                    all_empty = True
                    for key in key_list:
                        if data.get(key) not in ("", None):
                            all_empty = False
                            break
                    if all_empty:
                        return False
                return True
            if not check_json_keys_not_empty(json_data , key_list):
                dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                table = dynamodb.Table("AnnotationsInfo")
                result = table.update_item(
                    
                    Key={'orgId':meta_dict.get('org_id'),'annotationKey':meta_dict.get('annotation_key')},
                    UpdateExpression='SET #status = :status',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={':status': 'Incomplete'}
                )
                file_name = f"{cleaned_filename_without_extension}-page{i+1}"
                json_page_not_included.append(file_name)
                continue
                
                    
            print(f"Pages not included because not annotated -> {json_page_not_included}")
    
            # Upload JSON object to S3
            response = s3.put_object(
                Body=json.dumps(json_object),
                Bucket=bucket_name,
                Key=s3_json_object_key
            )
            #print(response)
            json_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': s3_json_object_key},
                    ExpiresIn=5 * 365 * 24 * 60 * 60 
                )
            temp_json_url = f"https://{bucket_name}.s3.us-east-1.amazonaws.com/{s3_json_object_key}"
            #print(temp_json_url)
            json_urls.append(temp_json_url)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"JSON data uploaded successfully to S3: {json_url}")
            else:
                print(f"Failed to upload JSON data to S3: {json_url}. Status code: {response['ResponseMetadata']['HTTPStatusCode']}")
        except ClientError as e:
            print(f"An error occurred while uploading JSON data to S3: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    # print(f"json_urls : {json_urls}")
    # print(f"parent json len : {len(parent_json)}")
    # print(f"json url len : {len(json_urls)}")
    # if isinstance(parent_json, dict):
    #     return json_urls
    # elif isinstance(parent_json, list):
    #     if len(parent_json) == len(json_urls):
    #         return json_urls
    # else:
    #     return False
    return json_urls , len(json_urls) , json_page_not_included

def upload_jpeg_to_s3(url,s3_url, meta_dict , page_not_included):
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    object_key = parsed_url.path.lstrip('/')
    object_key_without_extent = object_key[:-4]
    parts = object_key.split('/')

    # Remove the last part
    new_path = '/'.join(parts[:-2])
    #print(f"json url : {url}")
    
    jpeg_files = []
    jpeg_dict = []
    #page_count = int(meta_dict.get('page_count'))
    uploaded_time = meta_dict.get('uploaded_time')
    upload_date = meta_dict.get('upload_date')
    annotation_key = meta_dict.get('annotation_key')
    pdf_name = meta_dict.get('file_name')
    org_id = meta_dict.get('org_id')
    user_id = meta_dict.get('user_id')
    parent_id = meta_dict.get('annotation_key')
    print(parent_id)
    print(type(parent_id))
    bucket_name = meta_dict.get('bucket_name')
    document_type = meta_dict.get('document_type')
    pdf_name_without_extension = os.path.splitext(pdf_name)[0]
    
    image_path = os.path.join(pdf_file_path , "images")
    #print(f"image_path {image_path}")
    if not os.path.exists(image_path):
        os.makedirs(image_path)
    jpeg_files = os.listdir(image_path)
    jpeg_files = [item for item in jpeg_files if not any(item.startswith(prefix) for prefix in page_not_included)]
    sorted_files = sorted(jpeg_files, key=lambda x: int(x.split('-page')[1].split('.')[0]))
    print(sorted_files)
    jpeg_urls = []
    def extract_page(s):
        match = re.search(r'page\d+', s)
        return match.group() if match else None
    for i , jpeg_file in enumerate(sorted_files):
        page_number = extract_page(jpeg_file)
        s3_object_key = f"{new_path}/jpeg/{pdf_name_without_extension}-{page_number}.jpeg"
        
        img_path = os.path.join(pdf_file_path , "images" , jpeg_file)
        #print(f"img path : {img_path}")
        s3.put_object(Body=open(img_path, 'rb'), Bucket=bucket_name, Key=s3_object_key)
        jpeg_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': s3_object_key},
                    ExpiresIn=5 * 365 * 24 * 60 * 60 
                )
        
        temp_jpeg_url = f"https://{bucket_name}.s3.us-east-1.amazonaws.com/{s3_object_key}"
        jpeg_urls.append(temp_jpeg_url)
        #print(f"{jpeg_url} {temp_jpeg_url}")
        #print(f"jpeg_urls : {jpeg_urls}")
    for i , jpeg_file in enumerate(sorted_files):
        parts = jpeg_file.split('-page')
        match = re.search(r'-page\d+', jpeg_file)

        
        page_part = match.group(0)[1:] 

        before_page = parts[0]
        jpeg_dict.append({
            'org_id':org_id,
            'user_id':user_id,
            'parent_id':parent_id,
            'file_type':"jpeg",
            's3_url':jpeg_urls,
            'json_url':url,
            'file_name':jpeg_file,
            'page_count':i+1,
            'annotation_key':f"{annotation_key}-{page_part}",
            'bucket_name':bucket_name,
            'uploaded_time':uploaded_time,
            'upload_date':upload_date,
            'document_type':document_type
        })
    return jpeg_dict
 
def vendor_name(meta_dict):
    try:
        jsonurl = meta_dict.get('json_url')
        if jsonurl == None:
            print("No json Url is present")
        else:
            signed = signed_urls(jsonurl)
            #print(signed)
            response = json.loads(requests.get(signed).text)
            for r in response:
                #print(r)
                vendor = r.get('header').get('vendor')
                invoice_number = r.get('header').get('invoiceNumber')
                print(invoice_number)
                return vendor , invoice_number
    except:
        print("json is wrong formatted")
        #pass
    
def update_children_to_dynamo_db(jpeg_dict):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table("AnnotationsInfo")
    vendor , invoice_number = vendor_name(meta_dict)
    now = dt.utcnow()
    curr_timestamp = f"{now.year}{now.month:02}{now.day:02}{now.hour:02}{now.minute:02}"
    #curr_date = f"{dt.utcnow().month}/{dt.utcnow().day}/{dt.utcnow().year}"
    #curr_time = f"{dt.utcnow().hour}:{dt.utcnow().minute}:{dt.utcnow().second} {'AM' if dt.utcnow().hour < 12 else 'PM'}"
    #print(curr_timestamp)
    print(f"Vendor Name :  {vendor}")
    for i , item in enumerate(jpeg_dict):
        try:
            try:
                s3_url = item.get('s3_url')[i]
            except Exception as e:
                s3_url = None
                print(f"S3-url me locha {e}")
            json_url = None
            try:
                #print(f"length of json_url : {type(item.get('json_url'))}")
                if isinstance(item.get('json_url'),list):
                    json_url = item.get('json_url')[i]
                    print(f"The json url of {json_url} of {i+1} page")
                else:
                    json_url = item.get('json_url')
                    print(f"The json url of {json_url} of {i+1} page")
            except Exception as e:
                continue
                #print(f"jsonUrl me locha {e}")
            if json_url:
                response = table.put_item(
                    Item = {
                    'orgId': item.get('org_id'),
                    'userId': item.get('user_id'),
                    'parentId': item.get('parent_id'),
                    'fileName': item.get('file_name'),
                    's3Url': s3_url,
                    'jsonUrl': json_url,
                    'annotationKey': item.get('annotation_key'),
                    'fileType': "image/jpeg",
                    'fileStatus':"Complete",
                    'vendorName': vendor,
                    'status': "notUsedForTraining",
                    'bucketName':item.get('bucket_name'),
                    'uploadDate':item.get('upload_date'),
                    'timestamp':curr_timestamp,
                    'uploadedTime':item.get('uploaded_time'),
                    'documentType':item.get('document_type'),
                    'documentNumber':invoice_number
                    }
                )
            #print(f"Items : {item}")
                print(f"Successfully data uploaded page {i+1}..! on Dynamo DB")
        except Exception as e:
            print(f"Error in page {i} upload_to_dynamoDB: {e}")
            
            

def delete_folder():
    if os.path.isdir(pdf_file_path):
        shutil.rmtree(pdf_file_path)
    print("possibly remove all the files and folder of the specified path")

if __name__ == "__main__":

    meta_dicts = dynamo_db('AnnotationsInfo')
    print(len(meta_dicts))
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
            
            child_entries = check_child_record("AnnotationsInfo" , meta_dict)
        
            
            if page_count == child_entries:
                
                print(f"Children of {meta_dict.get('file_name')} pdf exist already on Dynamodb")
                delete_folder()
                continue
            else:
                convert_pdf_to_jpeg(page_count , meta_dict)
        except Exception as e:
            print(f"Exception out of three functions : {e}")

        jpeg_dict = ""   
        try:
            json_url = meta_dict.get('json_url')
            #print(f"dump_parent_json : {dump_parent_json_into_children(json_url)}")
            url_list, json_url_length , page_not_included = dump_parent_json_into_children(meta_dict , json_url)
            page_counted = pdf_page_count(pdf_path)
            #print(f"page_count {page_counted} and its type {type(page_counted)})")
            #print(f"json_url_length {json_url_length} and its type {type(json_url_length)}")
            s3_url = meta_dict.get('s3_url')
            #if page_counted == json_url_length:
            jpeg_dict = upload_jpeg_to_s3(url_list ,s3_url ,  meta_dict, page_not_included)
                #print(f"jpeg_dicts {jpeg_dict}")
            '''
            else:
                print(f"Total {page_count - json_url_length} page's json is not found , so the pdf {meta_dict.get('file_name')} is not able to upload on S3")
                dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                table = dynamodb.Table("AnnotationsInfo")
                result = table.update_item(
                    
                    Key={'orgId':meta_dict.get('org_id'),'annotationKey':meta_dict.get('annotation_key')},
                    UpdateExpression='SET #status = :status',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={':status': 'Incomplete'}
                )
           
            '''    
        except Exception as e:
            if json_url == None:
                print("json_url is not fetched")
            else:
                print(f"Exception at function dump_parent_json: {e}")
                
       
        try:
            update_children_to_dynamo_db(jpeg_dict)
        except Exception as e:
            print(f"Exception : {e}")
            #print(f"Page count is not defined for this file {meta_dict.get('file_name')}")
            
        delete_folder()
   
    sys.stdout.close()    
    
    
