"""
file: api_request.py
description: Sample python code for utilizing the HMS API, with authentication.
Simplified for readability (no error handling or  statements)
date: 12-03-2019
"""
import requests
import json
import time
from copy import deepcopy
from helpers import Helper
from datetime import datetime
import os
import logging
from logging import handlers,basicConfig



class HMS:
    """
    Python sample code for accessing the HMS web API. Functions as a CLI tool.
    If download is successful, data will be written to a json file.
    """
    server='https://ceamdev.ceeopdev.net' #'https://qed.epacdx.net/'#
    request_url = server+"/hms/rest/api/v3/"
    data_url = server+"/hms/rest/api/v2/hms/data?job_id="
    swagger_url = server+"/hms/api_doc/swagger/"

    login_url = server+"/login/?next=/hms/"
    
    
    user = 'hmsadmin'
    password = 'hmsAdmin2020!'

    request_body = {}
    task = None
    task_id = None
    result = None
    #helpers()
    def __init__(self, requestdict={}):
        """
        Initialization of the HMS request class object.
        :param component: HMS data component, valid values are 'hydrology', 'meteorology', and 'workflow'
        :param dataset: HMS component dataset, each component contains multiple datasets that can be found on the
        website or from the swagger api.
        :param source: Component dataset data source.
        :param start_date: Time series start date.
        :param end_date: Time series end date.
        :param geometry: Time series location, type and format depend on dataset source.
        :param timestep: Time series timestep, valid values are 'default', 'daily', 'weekly', 'monthly', 'yearly'
        """
        '''try:
            self.logger = logging.getLogger(__name__)
            self.logger.info('api_request.HMS using existing logger')
        except:'''
        '''pid=os.getpid()
        self.cwd=os.getcwd()
        self.logdir=os.path.join(self.cwd,'log')
        if not os.path.exists(self.logdir):os.mkdir(self.logdir)
        handlername=os.path.join(self.logdir,f'api_request_HMS-{pid}.log')
        logging.basicConfig(
            handlers=[logging.handlers.RotatingFileHandler(handlername, maxBytes=10**7, backupCount=1)],
            level=logging.DEBUG,
            format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
            datefmt='%Y-%m-%dT%H:%M:%S')'''
        self.logger = logging.getLogger(__name__)
        self.logger.info('new logger started for api_request.HMS object')


        
        
        
        
        self.csrftoken = None
        self.cookies = None
        
        self.component = requestdict.pop('component')
        self.dataset = requestdict.pop('dataset')
        try:
            self.output_name=requestdict.pop('output_name')
        except:
            print("requestdict['output_name'] cannot be found")
            self.output_name = self.component+'-'+self.dataset
        
        
        self.requestdict=requestdict
        
        self.login_url = self.login_url + self.component + "/" + self.dataset + "/"
        
        
        
        self.login()
        #helpers.__init__(self)
        self.set_request_body()
        
    def login(self):
        i = requests.get(self.login_url)
        self.csrftoken = i.cookies["csrftoken"]
        self.cookies = i.cookies
        next_url = "/hms/" + self.component + "/" + self.dataset + "/"
        header = {"Referer": self.login_url}
        request_data = {"username": self.user, "password": self.password, "csrfmiddlewaretoken": self.csrftoken, "next": next_url}
        login = requests.post(url=self.login_url, data=request_data, cookies=self.cookies, headers=header)
        self.cookies = login.history[0].cookies

    def set_request_body(self):
        """
        Sets the input parameters to the request json object obtained from the hms swagger api documentation.
        """
        
        # GET request of the swagger api documentation
        checkcount=3
        for i in range(checkcount):
            try:
                request=requests.get(self.swagger_url)
                request.raise_for_status()
                self.logger.debug(f'from api_request.py: request.status')
                swagger_json = json.loads(request.text)
                break
            except:
                self.logger.exception('')
                if i==checkcount-1:
                    assert False,'error with swagger_url request'
                time.sleep(1)
                    
        
        # Request body example for the specified component and dataset
        self.request_body = swagger_json["paths"]["/api/" + self.component + "/" + self.dataset]["post"]["requestBody"]["content"]["application/json"]["example"]
        #test=json.dumps(self.request_body)
        #self.default_request_body=deepcopy(self.request_body)
        
        #for key,val in self.requestdict.items():
            #self.request_body[key]=val
        #print('about to do do_dict_overide')
        self.request_body=Helper().do_dict_override(self.request_body,self.requestdict,verbose=0)
        self.requestdict['output_name']=self.output_name#adding it back in now that requestdict has been used to form the request_body.
        #print('self.request_body:',self.request_body)
        '''self.request_body["source"] = self.source
        self.request_body["dateTimeSpan"] = {
            "startDate": self.start_date,
            "endDate": self.end_date
        }
        self.request_body["geometry"] = self.geometry
        self.request_body["temporalResolution"] = self.timestep'''

    def submit_request(self):
        """
        Sends the request for data to the HMS API which triggers a new task.
        Request response returns a task ID and task status.
        """
        # POST request body
        params = json.dumps(self.request_body)
        # POST request to execute task
        header = {"Referer": self.request_url + self.component + "/" + self.dataset + "/"}
        request_data = requests.post(url=self.request_url + self.component + "/" + self.dataset + "/",
                                     data=params, cookies=self.cookies, headers=header)
        try:
            self.task = json.loads(request_data.text)
        except:
            self.logger.exception(f'request_data.text:{request_data.text}')
            assert False,'submit request failed'
        self.task_id = self.task["job_id"]
        print('self.task_id: ',self.task_id)
        self.geturl=self.data_url+self.task_id
        
    '''def do_geturl(self):
        self.result='none'
        return self.geturl'''
    
    def savejson(self,obj,path):
        with open(path,'w') as f:
            json.dump(obj,f)
    
    
    def get_data(self,recheck=1):
        """
        Queries the HMS data endpoint for the tasks current status.
        """
        delay = 20
        time.sleep(delay)
        # GET request to query task status and data retrieval
        
        r1 = requests.get(url=self.data_url + self.task_id)
        try:
            r1text=json.loads(r1.text)
        except:
            r1text=json.load(r1.text)
        
        
        '''with open('r1text_data.txt','w') as json_file:
            json.dump(r1text,json_file)'''
        
        
        self.result = r1text
        dir0=os.path.join(os.getcwd(),'results','0')
        if not os.path.exists(dir0):os.mkdir(dir0)
        self.savejson(r1text,os.path.join(dir0,self.task_id+'rawsave.json'))
        if self.result["status"] == "SUCCESS":
            print("HMS task successful")
            self.jsonresult=self.unpackresult(r1text)
            
        elif self.result["status"] == "PENDING":
            # print("Error: task is pending")
            if recheck:
                self.get_data()
        elif self.result["status"] == "STARTED":
            if recheck:
                self.get_data()
        elif self.result["status"] == "FAILURE":
            print("Error: Task failed to complete")
        return
    
    def unpackresult(self,r1text):
            data=r1text['data']
            if type(data) is str:
                jsondict=json.loads(r1text['data'])
                r1text['data']=jsondict
            elif type(data) is dict:
                pass
            else:
                assert False, 'expecting dict or str but data is {type(data)}. {data}'
                #jsondict=json.loads(r1text['data'])
                
            # self.savejson(jsondict,'testsave2.json')
            
            '''datadict=jsondict['data']
            tabledict=jsondict['table']

            for geogkey in datadict:
                for dictstring in datadict[geogkey]:
                    datadict[geogkey]=json.loads(dictstring)

            for geogkey in tabledict:
                tabledict[geogkey]=json.loads(tabledict[geogkey])
            '''
            
            return r1text
            
            
            
        
            
            
            

    def save_data(self, file_name=None):
        """
        Writes the downloaded data to json file.
        :param file_name: Optional file name for the data output.
        :return: None
        """
        if self.result is None:
            return
        if file_name is None:
            file_name = self.output_name
        with open(file_name, 'w') as json_file:
            json.dump(self.result["data"], json_file)


def main():
    # API inputs
    cmp = "meteorology"             # hydrology, meteorology
    ds = "precipitation"            # dataset
    src = "nldas"                   # dataset data source
    date0 = "2010-01-01"            # timeseries start date
    date1 = "2010-01-15"            # timeseries end date
    geometry = {                    # latitude and longitude of area of interest
        "point": {
            "latitude": 33.925,
            "longitude": -83.355
        }
    }
    ts = "daily"                    # timestep
    
    requestdict={'component':cmp,
                 'dataset':ds,
                 'source':src,
                 'dateTimeSpan':{
                     'startDate':date0,
                     'endDate':date1
                     },
                 'geometry':geometry, 
                 'temporalResolution':ts,
                 'output_name': 'API.txt' 
                     }
                 
                 
    hms = HMS(requestdict=requestdict)     # initializes request and obtains login cookies (csrf token and sessionid)
    print('submitting request')
    hms.submit_request()                                    # submits the request for data, using the initialized values
    print('getting data')
    hms.get_data()                                          # performs long polling (5 second delay) to check the status of the data request
    print('saving data')
    hms.save_data()                                         # and saves the completed results to file, specified by self.output_name or
                                                            # by the an optional argument parameter in get_data()


if __name__ == "__main__":
    main()
