import csv
import requests
import json
import api_key

# The file is a extract from Datadog of all the failed APIs for the /deals/v1/deal/ endpoint
file_name = 'failed-apis.csv'
url = 'https://api.hubapi.com/deals/v1/deal/'
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': 'Bearer {}'.format(api_key.hubspot_api_key)}
processed = []
errors = []

try:
    # Open the CSV file
    with open(file_name, mode='r') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.reader(csv_file)
        
        # Loop through each row in the CSV
        for row_number, row in enumerate(csv_reader, start=1):
            # Ensure the row has at least 4 fields
            if len(row) >= 4:
                # Access the 4th field
                fourth_field = row[3]
                
                # Split by the string "data:"
                split_result = fourth_field.split("data:")
                
                # Print the result of the split
                if len(split_result) > 1:
                    payload = split_result[1]
                    # Strip of the ' from star and end
                    payload = payload[2: len(payload)-1]
                    # Change the True and False to lowercase to be JSON compliant
                    payload = payload.replace("True", "true")
                    payload = payload.replace("False", "false")
                    # print(payload)

                    json_data = json.loads(payload)
                    name = json_data['properties'][0]['value']
                    # Only send the message once for each Family
                    # This is because the data logs show all retries ( max 10 )
                    if name not in processed:
                        processed.append(name)
                        print(name)
    
                        try:
                            # Post to hubspot
                            r = requests.post(url, data=payload, headers=headers)
                            print(r)
                        except Exception as post_error:
                            print(f"Error posting to Hubspot: {name} - {post_error}")
                            errors.append(name)


        count = len(processed)
        print(f"Processed: {count} rows.")
        count = len(errors)
        print(f"Failed: {count} rows.")

except FileNotFoundError:
    print(f"Error: The file '{file_name}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")