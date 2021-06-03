import json
import random
from datetime import datetime

class DataGenerator:
    def __init__(self, name, date, os, brand):
        self.__name = name
        self.__date = date
        self.__os = os
        self.__brand = brand

    def open_template(self):
        try:
            file = open('data_template.json',"r")
            data = json.load(file)
            file.close()
        except:
            data ={"user_id": "","usage_date": "","device":{"os":"","brand":""},"usages": [{"app_name":"","minutes_used":"","app_category":""}]}
        return data
    
    def generate_usage_data(self):
        app_list = ["Chrome" ,"Mozilla", "Whatsapp", "Facebook", "Spotify", "Pubg", "Pes2021", "Jira"]
        app_category = {'Chrome':'web_browser', "Mozilla": "web_browser", "Whatsapp": "entertainment", "Facebook": "entertainment", "Spotify": "entertainment_music", "Pubg": "entertainment_game", "Pes2021": "entertainment_game", "Jira": "task_management"}
        usage_data = []
        no_of_app = random.randint(5,8)
        print(no_of_app)
        single_app_info={}
        count=0
        for number in range(0,no_of_app):
            total_time_divide = 480//no_of_app
            app_name = random.choice(app_list)
            single_app_info['app_name'] = app_name
            single_app_info['minutes_used'] = random.randint(count*total_time_divide,(count+1)*(total_time_divide))
            single_app_info['app_category'] = app_category[app_name]
            usage_data.append(single_app_info)
            single_app_info={}
        return usage_data

    def generate_device_data(self):
        return({'os': self.__os, 'brand': self.__brand})

    def add_data(self):
        data = self.open_template()
        data['user_id'] = self.__name + "@tribes.ai"
        data['usage_date']= self.__date
        data['device']= self.generate_device_data()
        data['usages'] = self.generate_usage_data()
        return data

    


