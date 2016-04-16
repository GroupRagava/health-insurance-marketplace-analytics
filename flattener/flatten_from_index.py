# import dependencies
import time
import datetime
import requests
import pandas as pd
import logging
import csv
import json
from hurry.filesize import size


# set up some logging
reload(logging) # per http://stackoverflow.com/a/21475297/252671
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s: [%(levelname)s]: %(message)s',level=logging.DEBUG)


# helper function
def get_json_from_url(url):
    try:
        try: 
            head = requests.head(url)
            payload_size = size(int(head.headers['Content-Length']))
        except: 
            payload_size = "UNKNOWN"
        logging.info("... HTTP GET'ing: {0} [the file size is {1}]".format(url, payload_size))
        data = requests.get(url)
        data = data.json()
    except:
        logging.debug("!!! Request Failed")
        data = {}
    return data


array_of_index_files = [
    "https://fm.formularynavigator.com/jsonFiles/publish/11/47/cms-data-index.json",
    "https://www.gundersenhealthplan.org/QHPAPI/PNM/index.json",
    "https://www.caresource.com/vendor/cms/cms-data-index.json",
    "https://api.centene.com/ambetter/reference/cms-data-index.json"
]

all_drugs = []
all_plans = []
all_providers = []

drug_fields_to_include = ['rxnorm_id','drug_name']

provider_fields_to_include = ['npi','type','last_updated_on','phone','accepting','gender','facility_name']
provider_nested_fields_to_concatenate = ['addresses', 'specialty', 'languages', 'facility_type']
provider_nested_fields_in_name = ['prefix','first','middle','last','suffix']

plan_fields_to_include = ['plan_id_type','plan_id','marketing_name','summary_url','marketing_url','formulary_url','plan_contact','last_updated_on']
plan_nested_fields_to_concatenate = ['network','formulary','benefits']


# take a index.json file, fetch it
for index_url in array_of_index_files:
    logging.info("About to fetch index.json: {0}".format(index_url))
    index_json = get_json_from_url(index_url)
    
    # for all URLs in formulary_urls array, flatten into one array
    for formulary_url in index_json["formulary_urls"]:
        logging.info("About to fetch formulary.json: {0}".format(formulary_url))
        formulary_json = get_json_from_url(formulary_url)
        logging.info("... processing formulary.json: {0}".format(formulary_url))
        for drug in formulary_json:
            for plan in drug["plans"]:
                drug_plan_dict = {
                    '_index_url': index_url,
                    '_formulary_url': formulary_url
                }
                for field in drug_fields_to_include:
                    drug_plan_dict[field] = drug.get(field)
            drug_plan_dict.update(plan)
            all_drugs.append(drug_plan_dict)


    # for all URLs in plan_urls array, flatten into one array
    for plan_url in index_json["plan_urls"]:
        logging.info("About to fetch plans.json: {0}".format(plan_url))
        plan_json = get_json_from_url(plan_url)
        logging.info("... processing plan.json: {0}".format(plan_url))
        for plan in plan_json:
            plan_dict = {
                '_index_url': index_url,
                '_plan_url': plan_url,
            }
        for field in plan_fields_to_include:
            plan_dict[field] = plan.get(field)
        for field in plan_nested_fields_to_concatenate:
            field_value = plan.get(field)
            if(field_value == None):
                plan_dict['_n_{0}'.format(field)] = 0
                plan_dict[field] = None
            else:
                plan_dict['_n_{0}'.format(field)] = len(field_value)
                plan_dict[field] = json.dumps(field_value)
        all_plans.append(plan_dict)

    # for all URLs in provider_urls array, flatten into one array
    for provider_url in index_json["provider_urls"]:
        logging.info("About to fetch providers.json: {0}".format(provider_url))
        provider_json = get_json_from_url(provider_url)
        logging.info("... processing providers.json: {0}".format(provider_url))
        for provider in provider_json:
            for plan in provider["plans"]:
                provider_plan_dict = {
                    '_index_url': index_url,
                    '_provider_url': provider_url,
                }
                for field in provider_fields_to_include:
                    provider_plan_dict[field] = provider.get(field)
                for field in provider_nested_fields_in_name:
                    if(provider.get("name")):
                        provider_plan_dict['name.{0}'.format(field)] = provider["name"].get(field)
                    else:
                        provider_plan_dict['name.{0}'.format(field)] = None
                for field in provider_nested_fields_to_concatenate:
                    field_value = provider.get(field)
                    if(field_value == None):
                        provider_plan_dict['_n_{0}'.format(field)] = 0
                        provider_plan_dict[field] = None
                    else:
                        provider_plan_dict['_n_{0}'.format(field)] = len(field_value)
                        provider_plan_dict[field] = json.dumps(field_value)
                provider_plan_dict.update(plan)
                all_providers.append(provider_plan_dict)


# Save compiled data into CSV and/or JSON files
files_to_generate = [
    {'name': 'all_drugs', 'data': all_drugs, 'csv': True, 'json': False},
    {'name': 'all_plans', 'data': all_plans, 'csv': True, 'json': False},
    {'name': 'all_providers', 'data': all_providers, 'csv': True, 'json': False}
]

for file_info in files_to_generate:
    logging.info('Saving {0} to {0}.json and {0}.csv'.format(file_info['name']))
    if(len(file_info['data']) > 0):
        if file_info['json']: # save to JSON
            f = open('{0}.json'.format(file_info['name']), 'w')
            print >> f, file_info['data']
            f.close()
        if file_info['csv']: # save to CSV      
            keys = file_info['data'][0].keys()
            with open('{0}.csv'.format(file_info['name']), 'wb') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(file_info['data'])
    else: 
        logging.info('... did not save, there were no records to save')
