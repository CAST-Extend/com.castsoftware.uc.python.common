from cast_common.restAPI import RestCall
from cast_common.logger import Logger, INFO,DEBUG
from cast_common.util import format_table

from requests import codes
from pandas import ExcelWriter,DataFrame,json_normalize,concat
from typing import List
from json import loads,dumps

class Highlight(RestCall):

    _data = {}
    _tags = []
    _cloud = []
    _oss = []
    _elegance = []
    _apps_full_list = None
    _instance_id = 0

    log = None

    def __init__(self,  
                 hl_user:str=None, hl_pswd:str=None,hl_basic_auth=None, hl_instance:int=0,
                 hl_apps:str=[],hl_tags:str=[], 
                 hl_base_url:str='https://rpa.casthighlight.com', 
                 log_level=INFO, timer_on=False):

        if Highlight.log is None:
            Highlight.log = Logger('Highlight',log_level)

        reset_data = False
        if Highlight._instance_id is not hl_instance:
            self.log.debug(f'new instance id: {hl_instance}')
            Highlight._instance_id = hl_instance
            Highlight._data = {}
            reset_data = True

        super().__init__(base_url=f'{hl_base_url}/WS2/', user=hl_user, password=hl_pswd, 
                         basic_auth=hl_basic_auth,track_time=timer_on,log_level=log_level)

        # self._business = self._get_top_metric('businessValue')
        # self._cloud = self._get_top_metric('cloudValue')
        # self._oss = self._get_top_metric('openSourceSafty')
        # #self._elegance = self._get_top_metric('softwareElegance')

        if reset_data:
            Highlight._tags = self._get_tags()
            Highlight._apps_full_list = []

            # retrieve all applications from HL REST API
            Highlight._apps_full_list = DataFrame(self._get_applications())
#            Highlight._apps_full_list.dropna(subset=['metrics'],inplace=True)
            self.info(f'Found {len(Highlight._apps_full_list)} analyzed applications: {",".join(Highlight._apps_full_list["name"])}')
    
        # if the apps is not already of list type then convert it now
        if not isinstance(hl_apps,list):
            hl_apps=list(hl_apps.split(','))

        #if hl_apps is empty, include all applications prevously retrieved
        #otherwise filter the list down to include only the selected applications
        if len(hl_apps) > 0:
            self._apps = Highlight._apps_full_list[Highlight._apps_full_list['name'].isin(hl_apps)]
        else:
            self._apps = Highlight._apps_full_list

        self.info(f'{len(self._apps)} applications selected: {",".join(self._apps["name"])}')

        pass

    @property
    def app_list(self) -> DataFrame:
        return self._apps

    def _get_application_data(self,app:str=None):
        self.debug('Retrieving all HL application specific data')
        
        if str is None:
            df = self._apps
        else:
            df = self._apps[self._apps['name']==app]

        for idx,app in df.iterrows():
            app_name = app['name']
            self.debug(f'Loading Highlight data for {app_name}')
            if app_name not in Highlight._data:
                try:
                    self.debug(f'{app_name} - loading from REST')
                    Highlight._data[app_name]=self._get_app_from_rest(app_name)
                except KeyError as ke:
                    self.warning(str(ke))
                    pass
                except Exception as ex:
                    self.error(str(ex))
        pass

    def _get_metrics(self,app_name):
        try:
            if app_name not in self._apps['name'].to_list():
                raise ValueError(f'{app_name} is not a selected application')

            self._get_application_data(app_name)
            data = Highlight._data[app_name]
            metrics = data['metrics'][0]
            return metrics
        except KeyError as ke:
            self.error(f'{app_name} has no Metric Data')
            raise ke

    def _get(self,url:str,header=None) -> DataFrame:
        (status, json) = self.get(url,header)
        if status == codes.ok:
            return DataFrame(json)
        else:
            raise KeyError (f'Server returned a {status} while accessing {url}')

    def _get_applications(self):
        return self._get(f'domains/{Highlight._instance_id}/applications/', {'Accept': 'application/vnd.castsoftware.api.basic+json'} )
        
    def _get_tags(self) -> DataFrame:
        return DataFrame(self._get(f'domains/{Highlight._instance_id}/tags/'))

    # def _get_top_metric(self,metric:str) -> DataFrame:
    #     return DataFrame(self.post(f'domains/{Highlight._instance_id}/metrics/top?metric=cloudReady&order=desc',header={'Content-type':'application/json'}))

    def _get_app_from_rest(self,app:str) -> dict:
        return self._get(f'domains/{Highlight._instance_id}/applications/{self.get_app_id(app)}')

    def get_app_id(self,app_name:str) -> int:
        """get the application id

        Args:
            app_name (str): application name 

        Raises:
            KeyError: application not found

        Returns:
            int: highlight application id
        """
        if len(self._apps)==0:
            self._apps = self._get_applications()

        series = self._apps[self._apps['name']==app_name]
        if series.empty:
            raise KeyError (f'Highlight application not found: {app_name}')
        else:
            return int(series.iloc[0]['id'])
            
    def get_tag_id(self,tag_name:str):
        """get the application id

        Args:
            tag_name (str): tag name 

        Raises:
            KeyError: tag not found

        Returns:
            int: highlight tag id
        """
        if len(Highlight._tags)==0:
            Highlight._tags = self._get_tags()

        series = Highlight._tags[Highlight._tags['label']==tag_name]
        if series.empty:
            raise KeyError (f'Highlight tag not found: {tag_name}')
        else:
            return int(series.iloc[0]['id'])
            
    def add_tag(self,app_name,tag_name) -> bool:
        """Add tag for application 

        Args:
            app_name (_type_): applicaton name
            tag_name (_type_): tag name
        Returns:
            bool: True if tag added
        """
        app_id = self.get_app_id(app_name)
        tag_id = self.get_tag_id(tag_name)

        url = f'domains/{Highlight._instance_id}/applications/{app_id}/tags/{tag_id}'
        (status,json) = self.post(url)
        if status == codes.ok or status == codes.no_content:
            return True
        else:
            return False 
    
    def get_tech_debt_advisor(self,order:List[str],tag:str=None) -> DataFrame:
        #https://rpa.casthighlight.com/WS2/domains/1271/technicalDebt/aggregated?tagIds=685&activePagination=false&pageOffset=0&maxEntryPerPage=9999&maxPage=999&order=healthFactor&order=alert
        tag_part = ""
        if tag is not None:
            tag_part = f'tagIds={self.get_tag_id(tag)}&'
        url = f'domains/{Highlight._instance_id}/technicalDebt/aggregated?{tag_part}&activePagination=false&pageOffset=0&maxEntryPerPage=9999&maxPage=999'
        for o in order:
            url=f'{url}&order={o}'
        
        (status, json) = self.get(url)
        if status == codes.ok:
            d1 = json_normalize(json,['subLevel'], max_level=1)
            d1.rename(columns={'id':'Health Factor'},inplace=True)
            d1.drop(columns=['levelType'], inplace=True)

            d2=d1.explode('subLevel')
            d2=json_normalize(d2.to_dict('records'), max_level=1)
            d2=d2.rename(columns={'subLevel.id':'Alert','subLevel.detail':'detail'})
            d2=d2.drop(columns=['subLevel.levelType'])

            d3=json_normalize(d2.to_dict('records'),['detail'],meta=['Health Factor','Alert'])
            d3=d3.rename(columns={'application_id':'App Id','effort':'Effort'})
            d3=d3.drop(columns=['counter'])

            return d3

        else:
            raise KeyError (f'Server returned a {status} while accessing {url}')



    def get_cloud_detail(self,app_name:str)->DataFrame:
        """Highlight cloud ready data

        Args:
            app_name (str): name of the application

        Returns:
            DataFrame: flattened version of the Highlight cloud ready data
        """
        try:
            return json_normalize(self._get_metrics(app_name)['cloudDetail'],['cloudReadyDetails'],meta=['technology','cloudReadyScan'])
        except KeyError as ke:
            self.warning(f'{app_name} has no Cloud Ready Data')
            return None

    def get_tech_detail(self,app_name:str)->DataFrame:
        """Highlight technology data

        Args:
            app_name (str): name of the application

        Returns:
            DataFrame: flattened version of the Highlight technology data
        """
        try:
            return json_normalize(self._get_metrics(app_name)['technologies'],['greenIndexDetails'],meta=['technology','greenIndexScan'])
        except KeyError as ke:
            self.warning(f'{app_name} has no Green IT Data')
            return None
        

#hl = Highlight('n.kaplan+MerckMSD@castsoftware.com','vadKpBFAZ8KIKb2f2y',4711,hl_base_url='https://cloud.casthighlight.com',log_level=DEBUG)


# class A (Highlight):
#         pass

# class B (Highlight):
#         pass

# print ('class A')
# hl = A('n.kaplan+MerckMSD@castsoftware.com','vadKpBFAZ8KIKb2f2y',4711,hl_base_url='https://cloud.casthighlight.com',log_level=DEBUG)
# df = hl.get_green_detail('TPS')

# try:
#     print ('class B')
#     hl2 = B('n.kaplan+MerckMSD@castsoftware.com','vadKpBFAZ8KIKb2f2y',4711,'Demo',hl_base_url='https://cloud.casthighlight.com',log_level=DEBUG)
#     df = hl2.get_green_detail('Demo')
#     df2 = hl2.get_green_detail('TPS')
# except ValueError as ve:
#     print (ve)

# pass


