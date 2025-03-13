import requests
import json
import pandas as pd
import boto3
import time
import logging


all_rows = []
products_all_rows = [] # List to store processed infrastructure data
synthetic_all_rows = []  # List to store processed synthetic data
apm_all_rows = []  # List to store processed apm service data
rum_all_rows = [] # List to store processed rum service data
bucket_name = "observability-chargeback-reports"
secret_name = "observability-chargeback-reports-api"
region_name = "eu-west-1"
	
# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Funcation to fetch secret values from AWS Secrets Manager.
 
def get_secret():
    # Create a Secrets Manager client to fetch secret values
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response['SecretString']
        return json.loads(secret_string)  # Parse and return as dictionary
    except Exception as e:
        print(f"Error fetching secret: {e}")
        raise e


secret_data = get_secret() # Store secret value in variable

api_access_token = {
    'Global Inc': 'gwD-d_V8xml61ndzspT4AQ'
}

# Function to set up headers for API calls
def get_headers(token):
    return {
        'X-SF-TOKEN': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }


# Function to upload a file to S3
def upload_to_s3(file_name, bucket_name, s3_object_name):
    try:
        s3_client = boto3.client('s3')  # Initialize S3 client
        s3_client.upload_file(file_name, bucket_name, s3_object_name)
        logger.info(f"File {file_name} uploaded to S3 bucket {bucket_name} as {s3_object_name}")
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")

# function to format and update a DataFrame get it from "fetch_sub_data" funcation.

def division_productwise_data(org, url_key, data, products_all_rows):
    # Define columns to keep based on `url_key`
    columns_map = {
        "hosts": [
            'AppID', 'Environment', 'host.name', 'os.type', 'aws_tag_Hostname', 'aws_instance_id', 'aws_state'],
        "container": [
            'aws_tag_AppID', 'aws_tag_Environment', 'host.name','host_kernel_name', 'container.status', 'k8s.container.name', 'k8s.cluster.name'],
        "aws": [
            'aws_tag_AppID', 'aws_tag_Environment', 'aws_tag_Hostname', 'host_kernel_name', 'namespace', 'aws_instance_id', 'aws_state', 'stat' ],
        "azure": [
            'azure_tag_AppID', 'azure_tag_Environment', 'azure_tag_Name', 'azure_os_type', 'azure_vm_id', 'aggregation_type' ]
    }


    # Iterate through data based on url_key type
    if url_key in columns_map:
        # Loop through the data records
        for record in data:
            if url_key == "aws":
                # condition based data filtering
                if (not record.get("aws_tag_Hostname", '') and not record.get("aws_instance_id", '')):
                    continue
                if record.get("namespace", '') != "AWS/EC2" or record.get("stat", '') != "count":
                    continue
            elif  url_key == "azure" :
                if record.get("aggregation_type", '') != "count":
                    continue
                if (not record.get("azure_tag_Name", '') and not record.get("azure_vm_id", '')):
                    continue

            # Skip adding the row for container if container.status is not running
            elif url_key == "container":
                if "container.status" in record and record.get("container.status", '') != "running":
                     continue

            # Skip adding the row for hosts if required fields are missing
            elif url_key == "hosts" and not record.get("host.name", '') and not record.get("aws_tag_Hostname", '') and not record.get("aws_instance_id", ''):
                continue

            # Determine the HOSTNAME for AWS
            if url_key == "aws":
                hostname = record.get("aws_tag_Hostname", '') or record.get("aws_instance_id", '')
            elif url_key == "azure":
                hostname = record.get("azure_tag_Name", '') or record.get("azure_vm_id", '')
            elif url_key == "hosts":
                hostname = record.get("host.name", '') or record.get("aws_tag_Hostname", '') or record.get("aws_instance_id", '')
            else:
                hostname = record.get(columns_map[url_key][2], '')


            # Create a row dictionary for each record
            row = {
                "AppID": record.get(columns_map[url_key][0], ''),
                "Environment": record.get(columns_map[url_key][1], ''),
                "Hostname": hostname,
                "OS": record.get(columns_map[url_key][3], ''),
                "Type": url_key,
                "Division": org,
                "Container_name": record.get(columns_map[url_key][5], 'NA') if url_key == 'container' else 'NA',
                "Cluster_name": record.get(columns_map[url_key][6], 'NA') if url_key == 'container' else 'NA',
                "Hosts_State": record.get(columns_map[url_key][6], 'NA') if url_key in ['hosts', 'aws'] else 'NA'
            }

            # Append the row to products_all_rows
            products_all_rows.append(row)

    # Convert rows to DataFrame and save to CSV
    df = pd.DataFrame(products_all_rows)

    # Ensure "Hosts_State" column is treated as a string
    df["Hosts_State"] = df["Hosts_State"].astype(str)

    # Exclude rows where Hosts_State is "{Code: 48,Name: terminated}"
    df = df[df["Hosts_State"] != "{Code: 48,Name: terminated}"]

    # Drop the "Hosts_State" column after filtering (if needed)
    df = df.drop(columns=["Hosts_State"])

    # Replace empty strings with 'unknown'
    df.replace(['', 'n/a', 'null', 'N/A', 'NULL'], 'unknown', inplace=True)
    # Replace NaN (null) values with 'unknown'

    df.fillna('unknown', inplace=True)

    # Split AWS, Azure & Hosts data from Container data
    df_hosts = df[df["Type"].isin(["aws", "hosts","azure"])].copy()  # Filter AWS, Azure & Hosts data
    df_container = df[df["Type"] == "container"].copy()  # Keep container data

    # Find and remove duplicate hostnames from AWS,Azare and Hosts
    duplicate_hostnames = df_hosts[df_hosts.duplicated(subset=["Hostname"], keep=False)]["Hostname"].unique()
    df_hosts_filtered = df_hosts.drop_duplicates(subset=["Hostname"], keep="last")

    # Concatenate the two datasets back together
    df_final = pd.concat([df_hosts_filtered, df_container], ignore_index=True)

    # Save the final DataFrame to CSV
    filename = f"splunk-o11y-infra-usage-data.csv"
    df_final.to_csv(f"/tmp/{filename}", index=False)

    # Upload the CSV to S3
    s3_object_name = f"otel/{filename}"

    upload_to_s3(f"/tmp/{filename}", bucket_name, s3_object_name)

# Function to fetch Infra data using sequential requests
def fetch_data(org_name, access_token, url):
    headers = get_headers(access_token)
    limit_per_page = 10000
    record_processed = 0
    total_records = 0
    counter = 0
    all_results = []  # Accumulated results
    count_log = f"Start Time: {pd.Timestamp.now()}\n"

    while True:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                total_records = data.get("count", 0)
                results = data.get("results", [])
                all_results.extend(result.get("customProperties", {}) for result in results)
                # Update processed record count
                record_processed = record_processed + limit_per_page;
                if total_records - record_processed < limit_per_page:
                   counter = counter+1
                if total_records - record_processed <= 0 or counter == 2:
                   break
            else:
                logger.error(f"Failed to fetch data. Status Code: {response.status_code}. Response: {response.text}")
                break
        except (json.JSONDecodeError, requests.RequestException) as e:
            logger.error(f"Request failed for {org_name}: {e}")
            break

    count_log += f"Count: {len(all_results)}\nEnd Time: {pd.Timestamp.now()}\n------------------------------------------\n"
    logger.info(count_log)
    return all_results

# Function to fetch all pages using page number for synthetic
def fetch_synthetic_data(org, token, url):
    all_tests_results = []  # List to store all fetched tests
    page = 1
    per_page = 200
    total_fetched = 0  # Counter to track total fetched

    while True:
        # Construct URL with page parameter
        endpoint = f"{url}?page={page}&per_page={per_page}"

        headers = get_headers(token)

        response = requests.get(endpoint, headers=headers)

        if response.status_code != 200:
            logger.error(f"Error: {response.status_code}, {response.text}")
            break  # Stop fetching if an error occurs

        data = response.json()

        # Append test results (adjust key if different in API response)
        tests = data.get("tests", [])
        all_tests_results.extend(tests)
        total_fetched += len(tests)

        # Stop if no more results (API returns empty list or no "tests" key)
        if not tests:
            break

        # Increment page number
        page += 1

    return all_tests_results

# Function to process synthetic data recevied from fetch_synthetic_data
def process_synthetic_data(org, all_tests_results):
    for test in all_tests_results:
        name = test.get("name", "Unknown")
        test_type = test.get("type", "Unknown")

        # Extract AppID from customProperties (default to "Unknown")
        app_id = next(
            (prop.get("value") for prop in (test.get("customProperties") or []) if prop.get("key") == "AppID"),
            "Unknown"
        )

        synthetic_all_rows.append({
            "AppID": app_id,
            "Name": name,
            "Type": test_type,
            "Division": org
        })

        df = pd.DataFrame(synthetic_all_rows)
        filename = f"splunk-o11y-synthetic-usage-data.csv"
        df.to_csv(f"/tmp/{filename}", index=False)
        s3_object_name = f"otel/{filename}"
        upload_to_s3(f"/tmp/{filename}", bucket_name, s3_object_name)

# Function to fetch APM service data
def fetch_apm_data(org, token, url):
    headers = get_headers(token)
    # Get current time in milliseconds. We can only fetch 8 days data
    end_time = int(time.time() * 1000)
    start_time = end_time - (8 * 24 * 60 * 60 * 1000)  # 30 days in milliseconds
    service_data = []

    # Define request payload with correct time range format
    payload = {
        "timeRange": f"{start_time}/{end_time}"
    }

    # Make API request
    response = requests.post(url, headers=headers, json=payload)

    # Check if request was successful
    if response.status_code == 200:
        response_json = response.json()
        service_data = response_json.get("data", {}).get("nodes", [])
    else:
        logger.error(f"Failed to fetch data: {response.status_code}")

    return service_data

# Function to process APM service data
def process_apm_data(org, service_data, apm_all_rows):

    for service in service_data:
        service_name = service.get("serviceName", "Unknown")
        inferred = service.get("inferred", "Unknown")
        service_type = service.get("type", "Unknown")

        # Append data to the list
        apm_all_rows.append({
            "ServiceName": service_name,
            "Inferred": inferred,
            "Type": service_type,
            "Division": org
        })

    # Convert to DataFrame
    df = pd.DataFrame(apm_all_rows)

    # Save to CSV
    filename = f"splunk-o11y-apm-usage-data.csv"

    df.to_csv(f"/tmp/{filename}", index=False)
    logger.info(f"CSV file '{filename}' created successfully!")
    s3_object_name = f"otel/{filename}"
    upload_to_s3(f"/tmp/{filename}", bucket_name, s3_object_name)
    logger.info(f"CSV file '{filename}' uploaded successfully!")

# Function to fetch RUM service data
def fetch_rum_data(org, token, url):
    headers = get_headers(token)

    # Make API request
    response = requests.get(url, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        response_json = response.json()
        rum_service_data = [item.get("dimensions", {}) for item in response_json.get("results", [])]
    else:
        logger.error(f"Failed to fetch data: {response.status_code}")

    return rum_service_data

# Function to process RUM service data
def process_rum_data(org, rum_service_data, apm_all_rows):

    for service in rum_service_data:
        service_name = service.get("app", "Unknown")
        service_type = service.get("sf_product", "Unknown")

        # Append data to the list
        rum_all_rows.append({
            "ServiceName": service_name,
            "Type": service_type,
            "Division": org
        })

    # Convert to DataFrame
    df = pd.DataFrame(rum_all_rows)

    # Save to CSV
    filename = f"splunk-o11y-rum-usage-data.csv"
    df.to_csv(f"/tmp/{filename}", index=False)
    logger.info(f"CSV file '{filename}' created successfully!")
    s3_object_name = f"otel/{filename}"
    upload_to_s3(f"/tmp/{filename}", bucket_name, s3_object_name)
    logger.info(f"CSV file '{filename}' uploaded successfully!")

# Function to fetch ID,s from datatable for total summary
def fetch_sub_data(access_token, url):
    host_id = None
    container_id = None
    headers = get_headers(access_token)

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])

            for item in results:  # Iterate over the correct list
                resource_type = item.get("customProperties", {}).get("resourceType")
                resource_id = item.get("id")

                if resource_type == "host":
                    host_id = resource_id
                elif resource_type == "container":
                    container_id = resource_id
        else:
            logger.error(f"Error fetch data [{response.status_code}]: {response.text}")

    except (json.JSONDecodeError, requests.RequestException) as e:
        logger.error(f"Request failed: {e}")  # Removed undefined org_name

    return host_id, container_id  # Ensure return values are present in all cases

# Function to fetch count from datatable for total summary
def fetch_total_data(access_token, total_url):
    """Fetch total monitored resources data from SignalFX API."""
    headers = get_headers(access_token)
    try:
        response = requests.get(total_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {})  # Extract "data" dictionary
        else:
            logger.error(f"Error fetching data [{response.status_code}]: {response.text}")
            return {}
    except (json.JSONDecodeError, requests.RequestException) as e:
        logger.error(f"Request failed: {e}")
        return {}

# Function to map ID,s and count with org for total summary
def extract_latest_values(host_id, container_id, total_data):
    """Extract the latest monitored values for host and container IDs."""

    host_count = total_data.get(host_id, [[None, None]])[-1][1]
    container_count = total_data.get(container_id, [[None, None]])[-1][1]

    # Convert to integers if values exist
    host_count = int(host_count) if host_count is not None else 0
    container_count = int(container_count) if container_count is not None else 0
    return host_count, container_count

def lambda_handler(event, context):
    logger.info("Lambda function started execution")
    try:
        for org, token in api_access_token.items():
            if token:

                subscription_data_url = {
                    "resources": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=sf_metric:sf.org.numResourcesMonitored',
                    "infra_data": 'https://api.us0.signalfx.com/v1/timeserieswindow?query=sf_metric:sf.org.numResourcesMonitored&resolution=1000'
                    }

                urls = {
                    "hosts": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=metric:cpu.utilization&startTime=-31d&endTime=Now&limit=10000',
                    "container": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=metric:container.memory.usage&startMS=latest&limit=10000',
                    "aws": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=sf_metric:CPUUtilization&startTime=-31d&endTime=Now&limit=10000',
                    "azure": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=metric:"Percentage CPU"&startTime=-31d&endTime=Now&limit=10000'
            }
                synthetic_url = {
                    "synthetic": 'https://api.us0.signalfx.com/v2/synthetics/tests'
            }

                apm_url = {
                    "apm": 'https://api.us0.signalfx.com/v2/apm/topology'
            }

                rum_url = {
                    "rum": 'https://api.us0.signalfx.com/v2/metrictimeseries?query=sf_metric:sf.org.rum.numSessions&endTime=Now&startTime=-30d'
            }

                for key, url in urls.items():
                    data = fetch_data(org, token, url)
                    if data:
                        try:
                            filename = division_productwise_data(org, key, data, products_all_rows)
                            logger.info(f"{data} for {key} for {org} saved to {filename}")
                        except Exception as e:
                            logger.error(f"Error saving infra data to CSV for {key}: {e}")
                    else:
                        logger.info(f"No infra data available for {key} for organization: {org}")

                for key, url in synthetic_url.items():
                    synthetic_data = fetch_synthetic_data(org, token, url)
                    if synthetic_data:
                        try:
                            process_synthetic_data(org, synthetic_data)
                            logger.info(f"synthetic data for {org} saved")
                        except Exception as e:
                            logger.error(f"Error saving synthetic data for {key}: {e}")
                    else:
                        logger.info(f"No synthetic data available for organization: {org}")

                for key, url in apm_url.items():
                    apm_data = fetch_apm_data(org, token, url)
                    if apm_data:
                        try:
                            process_apm_data(org, apm_data, apm_all_rows)
                            logger.info(f"APM data for {org} saved")
                        except Exception as e:
                            logger.error(f"Error saving synthetic data for {key}: {e}")
                    else:
                        logger.info(f"No APM data available for organization: {org}")

                for key, url in rum_url.items():
                    rum_data = fetch_rum_data(org, token, url)
                    if rum_data:
                        try:
                            process_rum_data(org, rum_data, rum_all_rows)
                            logger.info(f"rum data for {org} saved")
                        except Exception as e:
                            logger.error(f"Error saving rum data for {key}: {e}")
                    else:
                        logger.info(f"No rum data available for organization: {org}")


                host_id, container_id = fetch_sub_data(token, subscription_data_url["resources"])
                total_data = fetch_total_data(token, subscription_data_url["infra_data"])
                host_count, container_count = extract_latest_values(host_id, container_id, total_data)

                all_rows.append({
                       "Division": org,
                       "Total Hosts": host_count,
                       "Total container": container_count
                })

                # Convert to DataFrame
                df = pd.DataFrame(all_rows)
                # Save to CSV
                filename = f"splunk-o11y-infra-summary-usage-data.csv"
                df.to_csv(f"/tmp/{filename}", index=False)
                s3_object_name = f"otel/{filename}"
                logger.info(f"CSV file '{filename}' created successfully!")
                upload_to_s3(f"/tmp/{filename}", bucket_name, s3_object_name)

        logger.info(f"CSV file '{filename}' uploaded successfully!")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    logger.info("Lambda execution completed")
