import boto3
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import botocore.exceptions

# Initialize IAM and S3 clients
iam_client = boto3.client('iam')
s3_client = boto3.client('s3')

# Cache for role last used dates to avoid redundant API calls
role_last_used_cache = {}

def fetch_all_custom_iam_policies():
    paginator = iam_client.get_paginator('list_policies')
    custom_policies = []
    for page in paginator.paginate(Scope='Local', PaginationConfig={'PageSize': 50}):
        custom_policies.extend(page['Policies'])
    return custom_policies

def get_policy_last_used(policy_arn):
    try:
        response = iam_client.get_policy(PolicyArn=policy_arn)
        return response.get('Policy', {}).get('PolicyLastUsed', {}).get('LastUsedDate')
    except botocore.exceptions.ClientError as e:
        print(f"Error fetching last used date for policy '{policy_arn}': {e}")
        return None

def list_policy_attachments(policy_arn):
    roles = []
    paginator = iam_client.get_paginator('list_entities_for_policy')
    for page in paginator.paginate(PolicyArn=policy_arn, EntityFilter='Role', PaginationConfig={'PageSize': 50}):
        roles.extend(page['PolicyRoles'])
    return roles

def get_role_last_used(role_name):
    if role_name in role_last_used_cache:
        return role_last_used_cache[role_name]
    try:
        response = iam_client.get_role(RoleName=role_name)
        last_used = response['Role'].get('RoleLastUsed', {}).get('LastUsedDate')
        role_last_used_cache[role_name] = last_used
        return last_used
    except botocore.exceptions.ClientError as e:
        print(f"Error fetching last used date for role '{role_name}': {e}")
        role_last_used_cache[role_name] = None
        return None

def process_policy(policy):
    policy_name = policy['PolicyName']
    policy_arn = policy['Arn']
    policy_last_used = get_policy_last_used(policy_arn)
    attached_roles = list_policy_attachments(policy_arn)

    results = []
    if attached_roles:
        for role in attached_roles:
            role_name = role['RoleName']
            role_last_used = get_role_last_used(role_name)
            results.append({
                'PolicyName': policy_name,
                'PolicyArn': policy_arn,
                'PolicyLastUsed': policy_last_used,
                'AttachedRoleName': role_name,
                'RoleLastUsed': role_last_used
            })
    else:
        results.append({
            'PolicyName': policy_name,
            'PolicyArn': policy_arn,
            'PolicyLastUsed': policy_last_used,
            'AttachedRoleName': '',
            'RoleLastUsed': ''
        })
    return results

def export_to_s3(data, bucket_name, object_key):
    csv_buffer = io.StringIO()
    fieldnames = ['PolicyName', 'PolicyArn', 'PolicyLastUsed', 'AttachedRoleName', 'RoleLastUsed']
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())

def lambda_handler(event, context):
    # Replace with your desired S3 bucket name and object key
    bucket_name = 'fi-iam-roles-backup '
    object_key = 's3://fi-iam-roles-backup/Polices/custom_iam_policies_with_roles.csv'

    print("Fetching custom IAM policies...")
    policies = fetch_all_custom_iam_policies()
    print(f"Found {len(policies)} custom policies. Gathering details...")

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_policy = {executor.submit(process_policy, policy): policy for policy in policies}
        for future in as_completed(future_to_policy):
            policy_results = future.result()
            results.extend(policy_results)

    print(f"Exporting results to S3 bucket '{bucket_name}' with key '{object_key}'...")
    export_to_s3(results, bucket_name, object_key)
    print("Export complete.")
