# import dependencies
import time
import datetime
import requests
import pandas as pd
import logging
import csv
import json
import unicodecsv
from hurry.filesize import size

# some configuration variables
array_of_index_files = ["https://www.pfpdata.com/cms-data/76168-de/76168-index.json","https://www.getjsonfile.com/cms-data-index.json","https://fm.formularynavigator.com/jsonFiles/publish/11/47/cms-data-index.json","https://api.humana.com/v1/cms/index.json","https://www.bcbsal.org/cms/cms-data-index.json","https://provider-search.qualchoice.com/index.json","http://groupaccess.deltadentalil.com/cmsdata/plans.json","https://api.centene.com/ambetter/reference/cms-data-index.json","https://groupaccess.deltadentalil.com/cmsdata/index.json","https://cmsdata.providers4you.com/cms-data-index.json","https://www.unitedconcordia.com/QHP/cms-data-index.json","https://www.deltadental.com/CMSDirectory/index.json","https://secure.arkansasbluecross.com/doclib/QHP/index.json","https://www.dominiondental.com/files/index.json","http://www.healthnet.com/static/hhs_az_json_20439/index.json","https://azblue.com/JSON/Index.JSON","https://www.anthem.com/cms-data-index.json/Anthem_Data_Index.json","https://phxchoice.com/qhp-provider-formulary-APIS/data/","https://www.healthchoiceessential.com/cms-data-index.json","http://www.bcbsfl.com/DocumentLibrary/Providers/cms/cms-data-index.json","https://www.health-first.org/applications/dn/cms_json/index.json","http://www.molinahealthcare.com/en-US/JSON/index.json","http://www.fhcp.com/xml/cms-data-index.json","http://s-rm3.cigna.com/sites/json/cms-data-index.json","https://healthy.kaiserpermanente.org/static/health/json/technical_information/ga/cms-data-index.json","https://www.harkenhealth.com/cms-data-index.json","https://www.gundersenhealthplan.org/QHPAPI/PNM/index.json","https://esbgatewaypub.medica.com:443/rest/QHP/cms-data-index.json?HIOSID=93078&fmt=json","https://www.floridabluedental.com/cms-data/FL/cms-data-index.json","https://www.healthalliance.org/content/CmsJson/x.json","https://www.bcbsil.com/forms/il/index_il.JSON","http://www.dencap.com/cms-data-index.json","https://test.geoaccess.com/mrf/llh/index/2852606/cms-data-index.json","https://secure.phpni.com/PublishedDataService/PublishedDataService.svc/index","https://www.caresource.com/vendor/cms/cms-data-index.json","http://www.mdwise.org/MediaLibraries/MDwise/Files/json/index.json","https://siho.org/marketplace/cms-data-index.json","https://secure.bcbsks.com/BuyBlue/qhp-data/index.BCBSKS.json","https://secure.bcbsks.com/BuyBlue/qhp-data/index.Solutions.json","http://cmsapi.bluekc.com/cms-data-index.json","https://mercycarehealthplans.com/wp-content/uploads/JSON/cms-data-index.json","https://portal.vantagehealthplan.com/data/cms-data-index.json","https://www.healthoptions.org/machinereadable/index.json","http://www.bcbsm.com/content/dam/public/marketplace/json/2016/bcbsm/index.json","https://harborchoice.com/qhp-provider-formulary-APIS/data/","http://corp.mhplan.com/siteassets/meridian-choice-data/meridianchoice_index.json","http://www.phpmichigan.com/upload/templates/json/index.json","https://www.mclaren.org/Uploads/Public/Documents/HealthPlan/documents/CMSUploads/cms-data-index.json","http://test.emihealth.com/services/cms-data-index.json","http://www.solsticebenefits.com/CMS/index.json","https://enroll.pacificsource.com/MRF/MT/cms-data-index.json","https://www.bcbsmt.com/forms/mt/index_mt.JSON","http://www.bcbsnc.com/cms-data-index.json","https://www.bcbsnd.com/documents/10181/3178650/index.json/4bee201d-f96b-422a-8e5f-de864540f623","https://esbgatewaypub.medica.com:443/rest/QHP/cms-data-index.json?HIOSID=73751&fmt=json","https://www.sanfordhealthplan.org/json/index.json","https://esbgatewaypub.medica.com:443/rest/QHP/cms-data-index.json?HIOSID=20305&fmt=json","https://www.nebraskablue.com/~/media/files/cms/index.json","https://newjersey-healthrepublic-static.s3.amazonaws.com/machine_readable/cms-index-data.json","https://static.hioscar.com/ffm/cms-data-index.json","https://www.amerihealthnj.com/Resources/cms-data/ahnj-hmo/index.json","https://doctorfinder.horizonblue.com/cms-data/json/index.json","https://www.amerihealthnj.com/Resources/cms-data/ahnj-ic/index.json","https://www.inhealthohio.org/QHP-provider-formulary-APIs/data/index.json","http://www.healthspan.org/cms-data-index.json","https://providersearch.medmutual.com/cms-data-index.json","https://s3.amazonaws.com/aultcare.machinereadable/index.json","https://www.myparamount.org/machinereadablefiles/cms-data-index.json","https://www.upmchealthplan.com/coverage/cms/cms-data-index.json","https://www.bcbsil.com/forms/il/index_ok.JSON","https://www.upmchealthplan.com/coverage/cms/cms-data-index.zip","https://cdn.thehealthplan.com/content/CMS/index.json","https://www.ibx.com/scripts/custom/cms-data/ibc-qcc/index.json","https://www.pfpdata.com/cms-data/33709-hmk/33709-index.json","https://www.ibx.com/scripts/custom/cms-data/ibc-khpe/index.json","https://www.pfpdata.com/cms-data/36247-hsr/36247-index.json","https://www.capbluecross.com/resources/cms/QHPProviderFormularyAPIs/cms-data-index.json","https://www.pfpdata.com/cms-data/55957-fplic/55957-index.json","https://www.pfpdata.com/cms-data/70194-hhic/70194-index.json","http://www.avera.org/app/files/public/58726/index.json","http://www.bcbst.com/cms-data-index.json","https://my.firstcare.com/CMS/MachineReadableFormat/cms-data-index.json","https://www.bcbstx.com/forms/tx/index_tx.JSON","http://www.prominencehealthplan.com/index.json","https://swhp.org/plandoc","https://qhphub.com/cfhp/index.json","https://allegianchoice.com/docs/librariesproviders27/json","https://senderohealth.com/idealcareeng/json/jsonfiles/index.json","https://secure.ccok.com/json/index.json","http://selecthealth.org/shprovider/index.json","https://member.carefirst.com/carefirst-resources/machine-readable/Index.json","http://www.pchp.net/cms-data","https://apps2.optimahealth.com/public/cmsJSON/index.json","https://healthy.kaiserpermanente.org/static/health/json/technical_information/va/cms-data-index.json","https://marketplace.cms.gov/submission/","https://fm.formularynavigator.com/jsonFiles/publish/150/69/cms-data-index.json","https://fm.formularynavigator.com/jsonFiles/publish/7/85/cms-data-index.json","https://app.prevea360.com/MachineReadable/cms-data-index.json","https://esbgatewaypub.medica.com:443/rest/QHP/cms-data-index.json?HIOSID=57845&fmt=json","https://data.networkhealth.com/hix/index.json","http://secure.wecareforwisconsin.com/QHP-provider-formulary-APIs/cms-data-index.json","http://www.commongroundhealthcare.org/json/index.json","https://ghcscw.com/xsp/cms/index.json","https://www.pfpdata.com/cms-data/31274-wv/31274-index.json"]

drug_fields_to_include = ['rxnorm_id','drug_name']

provider_fields_to_include = ['npi','type','last_updated_on','phone','accepting','gender','facility_name']
provider_nested_fields_to_concatenate = ['addresses', 'specialty', 'languages', 'facility_type']
provider_nested_fields_in_name = ['prefix','first','middle','last','suffix']

plan_fields_to_include = ['plan_id_type','plan_id','marketing_name','summary_url','marketing_url','formulary_url','plan_contact','last_updated_on']
plan_nested_fields_to_concatenate = ['network','formulary','benefits']


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

all_drugs = []
all_plans = []
all_providers = []

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
    if(index_json.get("plan_urls") != None): # shouldn't need this per spec, but we do
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
    {'name': 'all_plans', 'data': all_plans, 'csv': True, 'json': True},
    {'name': 'all_providers', 'data': all_providers, 'csv': True, 'json': False}
]

for file_info in files_to_generate:
    logging.info('Saving {0} to {0}.json and {0}.csv'.format(file_info['name']))
    if(len(file_info['data']) > 0):
        if file_info['json']: # save to JSON
            f = open('{0}.json'.format(file_info['name']), 'w')
            print >> f, json.dumps(file_info['data'])
            f.close()
        if file_info['csv']: # save to CSV      
            keys = file_info['data'][0].keys()
            with open('{0}.csv'.format(file_info['name']), 'wb') as output_file:
                dict_writer = unicodecsv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(file_info['data'])
    else: 
        logging.info('... did not save, there were no records to save')
