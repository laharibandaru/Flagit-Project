import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
import os
import json
import http.client
from flagit.src.flagit import flagit
from datetime import datetime, timedelta
import re


class SoilFlaggerIterative:
    def __init__(self):
        load_dotenv()
        self.api_connection = http.client.HTTPSConnection('api.precisionsustainableag.org')
        self.save_as_excel = True
        self.chart_moisture = True
        self.codes = pd.read_csv("possibly_shepherded_sites.csv")
        #self.codes = pd.read_csv("possibly_shepherded_sites_test.csv")
        self.frequency = 0.25
        
    def run(self):
        file = 'all_flags.csv'
        if(os.path.exists(file) and os.path.isfile(file)):
            self.all_flags = pd.read_csv(file)
            os.remove(file)
        else:
            self.all_flags = pd.DataFrame(columns = ['uid', 'qflag'])

        self.flagged_uids = list(self.all_flags['uid'])
        self.numModified = 0
        self.numUnflagged = 0
        self.iterate_codes()
        self.all_flags.to_csv('all_flags.csv', index = False)

        print("Number of Unflagged Entries: " + str(self.numUnflagged))
        print("Number of Reflagged Entries: " + str(self.numModified - self.numUnflagged))
        print("Total Number of Modified Entries (including unflagged): " + str(self.numModified))


    def iterate_codes(self):
        for index, farm_row in self.codes.iterrows():

            self.code = farm_row["code"]
            self.subplot = farm_row["subplot"]
            self.treatment = farm_row["treatment"]
            print(self.code)

            soil_data = self.fetch_onfarm_api(
                '/onfarm/soil_moisture?output=json&type=tdr&code={}&subplot={}'.format(self.code, self.subplot))
            data_by_field = self.extract_soil_data(soil_data)

            df = pd.DataFrame(data_by_field)
            if not df.empty:
                df = df.sort_values(by='index')
                df = df.set_index('index')
                df = df[~df.index.duplicated(keep='first')]
                df = df[df['treatment'] == self.treatment]

                try:
                    df['timestamp'] = pd.to_datetime(df.index)
                except Exception:
                    print(df)

                self.run_flagit_by_sensor(df[df['center_depth'] == -5])
                self.run_flagit_by_sensor(df[df['center_depth'] == -15])
                self.run_flagit_by_sensor(df[df['center_depth'] == -45])
                self.run_flagit_by_sensor(df[df['center_depth'] == -80])

            
    def fetch_onfarm_api(self, uri):
        api_key = os.environ.get('X_API_KEY')
        api_headers = {'x-api-key': api_key}
        self.api_connection.request('GET', uri, headers=api_headers)
        api_res = self.api_connection.getresponse()
        api_data = api_res.read()
        json_api_data = api_data.decode('utf8')
        try:
            api_json_data = json.loads(json_api_data)
            return(api_json_data)
        except Exception:
            print(json_api_data)

    def extract_soil_data(self, soil_data):
        data_by_field = {
            'soil_moisture': [],
            'soil_temperature': [],
            'uid': [],
            'index': [],
            'is_vwc_outlier': [],
            'center_depth': [],
            'vwc_outlier_who_decided': [],
            'treatment': [],
        }

        for item in soil_data:
            if item.get('node_serial_no') and item.get('center_depth'):
                if item.get('vwc'):
                    data_by_field['soil_moisture'].append(
                        float(item.get('vwc')))
                    data_by_field['soil_temperature'].append(
                        item.get('soil_temp'))
                    data_by_field['uid'].append(
                        int(item.get('uid')))
                    data_by_field['index'].append(
                        item.get('timestamp'))
                    data_by_field['is_vwc_outlier'].append(
                        item.get('is_vwc_outlier'))
                    data_by_field['center_depth'].append(
                        item.get('center_depth'))
                    data_by_field['vwc_outlier_who_decided'].append(
                        item.get('vwc_outlier_who_decided'))
                    data_by_field['treatment'].append(
                        item.get('treatment'))

        return data_by_field
    
    def run_flagit_by_sensor(self, sensor_data):
        sensor_data = sensor_data.reset_index()#indices are for all sensor data, flagged or unflagged
        sensor_data['index_column'] = sensor_data.index 
        self.run_flagit(sensor_data)
    
    def run_flagit(self, sensor_data):
        pass_window_all = []
        change_window_all = []
        for index, row in sensor_data.iterrows(): #index is the index in sensor_data
            if not row['uid'] in self.flagged_uids: # unflagged
                self.numUnflagged += 1
                pass_range, change_range = self.get_entry_window(sensor_data, index)
                pass_window_all = pass_window_all + pass_range
                pass_window_all = [*set(pass_window_all)]
                change_window_all = change_window_all + change_range
                change_window_all = [*set(change_window_all)]
        if len(pass_window_all)>0:
            flagit_interface = flagit.Interface(sensor_data[sensor_data['index_column'].isin(pass_window_all)], frequency=0.25)
            flagit_results = flagit_interface.run()
            flagit_results = flagit_results[flagit_results['index_column'].isin(change_window_all)][['uid', 'qflag']]
            self.numModified += len(flagit_results.index)
            self.all_flags = pd.concat([self.all_flags, flagit_results])
            self.all_flags.drop_duplicates('uid', keep='last', inplace=True)
            

    def get_entry_window(self, sensor_data, curr_index): # change_window AND pass_window
        hours_12 = 24 * 7 * int(1 / self.frequency)
        hours_24 = 2 * hours_12
        hours_36 = 3 * hours_12

        pass_range = list(range(curr_index - hours_36, curr_index + hours_36+1))
        change_range = list(range(curr_index - hours_24, curr_index + hours_12+1))

        return pass_range,change_range
    




    def get_accuracy_stat(self):
        num_both_flagged = 0
        num_both_unflagged = 0

        num_flagit_flagged = 0
        num_manually_flagged = 0
        num_flagit_unflagged = 0
        num_manually_unflagged = 0

        for index, farm_row in self.codes.iterrows():

            code = farm_row["code"]
            subplot = farm_row["subplot"]
            treatment = farm_row["treatment"]
            print(code)

            soil_data = self.fetch_onfarm_api(
                '/onfarm/soil_moisture?output=json&type=tdr&code={}&subplot={}'.format(code, subplot))
            data_by_field = self.extract_soil_data(soil_data)

            df = pd.DataFrame(data_by_field)
            if not df.empty:
                df = df.sort_values(by='index')
                df = df.set_index('index')
                df = df[~df.index.duplicated(keep='first')]
                df = df[df['treatment'] == treatment]

                try:
                    df['timestamp'] = pd.to_datetime(df.index)
                except Exception:
                    print(df)
                
                # sorted by code, subplot, and treatment @ this point. 

                # percent flagit flagged is actually flagged

                for index, row in df.iterrows():
                    uidData = self.all_flags[self.all_flags['uid']==row['uid']]
                    flagitResult = self.all_flags[self.all_flags['uid']==row['uid']].iloc[0][['qflag']].iloc[0]
                    manualResult = row['is_vwc_outlier']

                    #both flagged
                    if flagitResult != "{'G'}" and manualResult:
                        num_both_flagged +=1
                    #both unflagged
                    if flagitResult == "{'G'}" and not manualResult:
                        num_both_unflagged +=1
                    #flagit flagged
                    if flagitResult != "{'G'}":
                        num_flagit_flagged +=1
                    #manually flagged
                    if manualResult:
                        num_manually_flagged +=1
                    #flagit unflagged
                    if flagitResult == "{'G'}":
                        num_flagit_unflagged +=1
                    #manually unflagged
                    if not manualResult:
                        num_manually_unflagged +=1

        print("Percent flagit flagged that was manually flagged: " + str(100*(num_both_flagged/num_flagit_flagged)) + "%")
        print("Percent flagit unflagged that was manually unflagged: " + str(100*(num_both_unflagged/num_flagit_unflagged)) + "%")
        print("Percent manually flagged that was flagit flagged: " + str(100*(num_both_flagged/num_manually_flagged)) + "%")
        print("Percent manually unflagged that was flagit unflagged: " + str(100*(num_both_unflagged/num_manually_unflagged)) + "%")

