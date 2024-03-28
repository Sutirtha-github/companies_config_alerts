import config
import datetime
import dotenv
import json
import os
import time
import traceback
import uuid
import pandas as pd
import pytz

from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def init_with_service_account():
    """
    Initialize the Firestore DB client using a service account
    :param file_path: path to service account
    :return: firestore
    """
    cred = credentials.ApplicationDefault()
    print("Google Env: ", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""):
        print("Using Env provided Credentials")
        cred = credentials.Certificate(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    # try:
    #     pf_fb = firebase_admin.get_app(config.PEAKFLO_PROJECT_DETAILS.get("clientName", "peakflo"))
    # except ValueError:
    firebase_admin.initialize_app(cred, {"projectId": config.PEAKFLO_PROJECT_DETAILS.get("projectName", "peakflo")})
    pf_fb = firestore.client()
    return pf_fb


def publish_message(project, topic, message):
    publisher = pubsub_v1.PublisherClient()
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""):
        cred = cred = service_account.Credentials.from_service_account_file(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "./serviceAccount.json"))
        publisher = pubsub_v1.PublisherClient(credentials=cred)
    topic_path = publisher.topic_path(project, topic)
    # Data must be a bytestring
    data = json.dumps(message).encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data)
    print("*"*80)
    print(future.result())
    print("*"*80)

def calculate_time_difference(timestamp_str, timezone, timestamp_format):
    timestamp = datetime.strptime(timestamp_str, timestamp_format)
    tz = pytz.timezone(timezone)
    timestamp = timestamp.astimezone(tz)
    current_time = datetime.now(tz)
    difference = current_time - timestamp
    return difference.days, current_time

def determine_time_format(time_str):
    formats = config.TIMESTAMP_FORMATS
    for fmt in formats:
        try:
            datetime.strptime(time_str, fmt)
            return fmt
        except ValueError:
            pass
    return None

def main(data, context):
    start_time = time.time()
    print("*"*80)
    print("Starting Execution on %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("*"*80)

    # Load the env variables
    dotenv.load_dotenv()
    print("*"*80)
    print("Env Variables: ")
    print("Credentials: ", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "./serviceAccount.json"))
    print("*"*80)


    # Use a service account
    print("*"*80)
    print("Creating Firestore Variables")
    print("*"*80)
    fbdb = init_with_service_account()

    print("*"*80)
    print("Configuration: ", config.CONFIG_CHECK)
    print("*"*80)

    
    collection_name = config.COLLECTION_NAME
    timezone = 'Asia/Kolkata'
    df = pd.DataFrame(columns = ['status','lastSyncedAt', 'AS','tenant'])

    scheduled_sync_status_list = []
    last_synced_at_list = []
    as_list = []
    tenant_list = []
    time_difference = -1
    
    try:
        tenants_path = fbdb.collection(collection_name).get()
        tenants_id = [tenant_path.id for tenant_path in tenants_path]
        for tenant_id in tenants_id:
            acc_sys_paths =  fbdb.collection(collection_name).document(tenant_id).collection('connects').get()
            num_acc = len(acc_sys_paths)
            if num_acc>0 :
                for path in acc_sys_paths:
                    try:
                        acc_dict = path.to_dict()
                        keys = acc_dict.keys()
                        # Check if scheduledSync field present
                        if 'scheduledSync' in keys and 'disconnectedAt' not in keys:
                            scheduled_sync_status = acc_dict['scheduledSync']['status']
                            if scheduled_sync_status == 'SYNC':
                                # Check if items field present in scheduledSync else sync time is undefined
                                if 'items' in acc_dict['scheduledSync'].keys():
                                    if path.id != 'msnav':
                                    # Check if invoices field present in scheduledSync.items else sync time is undefined
                                        if 'invoices' in list(acc_dict['scheduledSync']['items'].keys()) or 'invoice' in list(acc_dict['scheduledSync']['items'].keys()) or 'bill' in list(acc_dict['scheduledSync']['items'].keys()) or 'bills' in list(acc_dict['scheduledSync']['items'].keys()):
                                            if 'invoices' in acc_dict['scheduledSync']['items'].keys():
                                                invoice_bill = 'invoices'
                                            elif 'invoice' in acc_dict['scheduledSync']['items'].keys():
                                                invoice_bill = 'invoice'
                                            elif 'bill' in acc_dict['scheduledSync']['items'].keys():
                                                invoice_bill = 'bill'
                                            else:
                                                invoice_bill = 'bills'

                                        else:
                                            if 'vendor' in list(acc_dict['scheduledSync']['items'].keys()):
                                                invoice_bill = 'vendor'


                                        time_fld = 'None'
                                        '''
                                        sync time field can be one of [lastSyncedTime, lastSyncedAt, lastUpdatedTime] or some other variation,
                                        '''
                                        for key1 in list(acc_dict['scheduledSync']['items'][invoice_bill].keys()):
                                            if key1 in ['lastSyncedTime', 'lastSyncedAt', 'lastUpdatedTime']:
                                                time_fld = key1
                                                break
                                        time_difference = -1
                                        if time_fld != 'None':
                                            last_synced_at = str(acc_dict['scheduledSync']['items'][invoice_bill][time_fld])
                                            last_synced_at_formatted = last_synced_at
                                            if last_synced_at[-3] == ':':
                                                # Change the format from HH:MM to HHMM
                                                last_synced_at_formatted = last_synced_at[:-3] + last_synced_at[-2:]
                                            format = determine_time_format(last_synced_at_formatted)
                                            time_difference, current_time = calculate_time_difference(last_synced_at_formatted, timezone, format)
                                            if time_difference > 1:
                                                scheduled_sync_status_list.append(scheduled_sync_status)
                                                last_synced_at_list.append(last_synced_at)
                                                as_list.append(path.id)
                                                tenant_list.append(tenant_id)
                                        else:
                                            last_synced_at = 'None'
                                    else:
                                        last_synced_at = 'None'

                                    if last_synced_at == 'None':
                                        scheduled_sync_status_list.append(scheduled_sync_status)
                                        last_synced_at_list.append(last_synced_at)
                                        as_list.append(path.id)
                                        tenant_list.append(tenant_id)
                    except Exception:
                        pass    

        df['status'] = scheduled_sync_status_list
        df['lastSyncedAt'] = last_synced_at_list
        df['AS'] = as_list
        df['tenant'] = tenant_list

        if df.shape[0]>0:
            message = ""
            message += "🔄 companies_config Sync alerts :" + '\n'
            message += "⏰ Current Time - "+ str(current_time) +'\n'
            message += "Total count : " + str(df.shape[0]) + '\n\n\n'
            df_undef = df[df['lastSyncedAt']=='None']
            df_def = df[df['lastSyncedAt'] != 'None']

            message += "status : SYNC      lastSyncedAt : undefined(None) -\n\n"
            for acc_soft in df_undef['AS'].unique():
                message += "Accounting software : " + acc_soft + '\n'
                df_undef_as = df_undef[df_undef['AS']==acc_soft]
                for i in range(df_undef_as.shape[0]):
                    message += list(df_undef_as['AS'])[i] + ' - ' + list(df_undef_as['tenant'])[i] + ' - ' + list(df_undef_as['lastSyncedAt'])[i] + '\n'
                message += "Count : " + str(df_undef_as.shape[0]) + '\n\n'
            message += '*************************************************************' + '\n\n'
            message += "status : SYNC      lastSyncedAt : defined (>1 day) -" + '\n'
            for acc_soft in df_def['AS'].unique():
                message += "Accounting software : " + acc_soft + '\n'
                df_def_as = df_def[df_def['AS']==acc_soft]
                for i in range(df_def_as.shape[0]):
                    message += list(df_def_as['AS'])[i] + ' - ' + list(df_def_as['tenant'])[i] + ' - ' + list(df_def_as['lastSyncedAt'])[i] + '\n'
                message += "Count : " + str(df_def_as.shape[0]) + '\n\n'
            
        
            print("*"*80)
            print("Slack Message: ", message)
            print("*"*80)

            if message:
                pub_to_slack = {
                    "channel": os.environ.get("PUB_TOPIC", "slack"),
                    "projectId": config.PEAKFLO_PROJECT_DETAILS.get("projectName", "peakflo"),
                    "deliveryId": str(uuid.uuid4()),
                    "communication": {"to": "integrations"},
                    "data": {"message": message}
                }

                publish_message(config.NOTIFICATION_PROJECT_DETAILS.get("projectName", "notifications-stage-314905"), os.environ.get("PUB_TOPIC", "slack"), pub_to_slack)
                pass
    except Exception as _:
        print(traceback.print_exc())
        slack_message = "🔄 SYNC CHECK \n" + "Found error while executing Sync checks - Please check the logs \n"
        pub_to_slack = {
            "channel": os.environ.get("PUB_TOPIC", "slack"),
            "projectId": config.PEAKFLO_PROJECT_DETAILS.get("projectName", "peakflo"),
            "deliveryId": str(uuid.uuid4()),
            "communication": {"to": "integrations"},
            "data": {"message": slack_message}
        }
        print(slack_message)
        publish_message(config.NOTIFICATION_PROJECT_DETAILS.get("projectName", "notifications-stage-314905"), os.environ.get("PUB_TOPIC", "slack"), pub_to_slack)
    
    print("*"*80)
    print("Time Taken: %s seconds." % str(time.time() - start_time))
    print("*"*80)


if __name__ == "__main__":
    main('data', 'context')