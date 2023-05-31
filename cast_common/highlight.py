from cast_common.restAPI import RestCall
from cast_common.logger import INFO
from cast_common.util import format_table

from requests import codes
from pandas import ExcelWriter,DataFrame,json_normalize,concat
from typing import List
from json import loads

class Highlight(RestCall):

    _data = {}
    _apps = []
    _tags = []
    _cloud = []
    _oss = []
    _elegance = []
    _instance_id = None

    def __init__(self,  hl_user:str, hl_pswd:str, 
                 hl_instance:int,hl_apps:str=[],hl_tags:str=[], 
                 hl_base_url:str='https://rpa.casthighlight.com', 
                 log_level=INFO, timer_on=False):

        super().__init__(f'{hl_base_url}/WS2/', hl_user, hl_pswd, timer_on,log_level)

        self._hl_instance = hl_instance

        # self._business = self._get_top_metric('businessValue')
        # self._cloud = self._get_top_metric('cloudValue')
        # self._oss = self._get_top_metric('openSourceSafty')
        # #self._elegance = self._get_top_metric('softwareElegance')

        self._tags = self._get_tags()
        self._apps = DataFrame(self._get_applications())
        self.info(f'Found {len(self._apps)} applications')
        self._apps.dropna(subset=['metrics'],inplace=True)
        self.info(f'Found {len(self._apps)} analyzed applications')
        if len(hl_apps) > 0:
            self._apps = self._apps[self._apps['name'].isin(hl_apps)]
        self.info(f'{len(self._apps)} applications selected')

        self._data = {}
        # for idx,app in self._apps.iterrows():
        #     try:
        #         app_name = app['name']
                
        #         self.info(f'Loading Highlight data for {app_name}')
        #         self._data[app_name]= self._get_application_data(app_name)


                # domains = app_data[app_name]['domains']
                # metrics = app_data[app_name]['metrics'][0]
                # green_detail = json_normalize(metrics['greenDetail'],['greenIndexDetails'],meta=['technology','greenIndexScan'])
                # cloud_detail = metrics['cloudReadyDetail'][0]
                # technology_detail = metrics['technologies'][0]
                # vulnerability_detail = metrics['vulnerabilities'][0]
                # components_detail = metrics['components'][0]

                # app_data[app]={'domains':domains}

        #         pass
        #     except KeyError as ke:
        #         pass
        #     except Exception as ex:
        #         self.error(str(ex))
        # pass


        # file_name = r'e:/work/wellsfargo/tags.xlsx'
        # writer = ExcelWriter(file_name, engine='xlsxwriter')
        # summary_tab = format_table(writer,DataFrame(self._tags),'Tags')
        # writer.close()

        pass

    def _get_all_application_data(self):
        self.info('Retrieving all HL application specific data')
        self._data = {}
        for idx,app in self._apps.iterrows():
            try:
                app_name = app['name']
                self.info(f'Loading Highlight data for {app_name}')
                self._data[app_name]= self._get_application_data(app_name)
            except KeyError as ke:
                self.warning(str(ke))
                pass
            except Exception as ex:
                self.error(str(ex))
        pass

    def _get_metrics(self,app_name):
        try:
            if len(self._data)==0:
                self._get_all_application_data()
            data = self._data[app_name]
            metrics = data['metrics'][0]
            return metrics
        except KeyError as ke:
            self.error(f'{app_name} has no Metric Data')
            raise ke

    def _get(self,url:str) -> DataFrame:
        (status, json) = self.get(url)
        if status == codes.ok:
            return DataFrame(json)
        else:
            raise KeyError (f'Server returned a {status} while accessing {url}')

    def _get_applications(self):
        return self._get(f'domains/{self._hl_instance}/applications/')
        
    def _get_tags(self) -> DataFrame:
        return DataFrame(self._get(f'domains/{self._hl_instance}/tags/'))

    # def _get_top_metric(self,metric:str) -> DataFrame:
    #     return DataFrame(self.post(f'domains/{self._hl_instance}/metrics/top?metric=cloudReady&order=desc',header={'Content-type':'application/json'}))

    def _get_application_data(self,app:str) -> dict:
        return self._get(f'domains/{self._hl_instance}/applications/{self.get_app_id(app)}')

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
        if len(self._tags)==0:
            self._tags = self._get_tags()

        series = self._tags[self._tags['label']==tag_name]
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

        url = f'domains/{self._hl_instance}/applications/{app_id}/tags/{tag_id}'
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
        url = f'domains/{self._hl_instance}/technicalDebt/aggregated?{tag_part}&activePagination=false&pageOffset=0&maxEntryPerPage=9999&maxPage=999'
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
            return json_normalize(self._get_metrics(app_name)['greenDetail'],['cloudReadyDetails'],meta=['technology','cloudReadyScan'])
        except KeyError as ke:
            self.warning(f'{app_name} has no Cloud Ready Data')
            return None

    def get_green_detail(self,app_name:str)->DataFrame:
        """Highlight green it data

        Args:
            app_name (str): name of the application

        Returns:
            DataFrame: flattened version of the Highlight green it data
        """
        try:
            return json_normalize(self._get_metrics(app_name)['greenDetail'],['greenIndexDetails'],meta=['technology','greenIndexScan'])
        except KeyError as ke:
            self.warning(f'{app_name} has no Green IT Data')
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


#https://rpa.casthighlight.com/WS2/domains/1271/applications/188146/tags/679



# hl = Highlight('n.kaplan+MerckMSD@castsoftware.com','vadKpBFAZ8KIKb2f2y',4711,hl_base_url='https://cloud.casthighlight.com/')
# green_data = hl.get_green_details('TPS-2')
# pass


# hl = Highlight('n.kaplan+WellsFargo@castsoftware.com','mdSi20ty@02',1271,hl_base_url='https://rpa.casthighlight.com/')

# df = hl.get_tech_debt_advisor(['healthFactor','alert'],tag='Hamilton, Cliona')
# df['Effort']=df['Effort']/60/8
# df=df.rename(columns={'application_name':'Component'})
# #df['Technology']=''
# df['Technology']=df['Alert'].str.split('__').str[0]
# df['Rule']=df['Alert'].str.split('__').str[1]
# df['Rule']=df['Rule'].str.replace('_','').str.replace(r"(\w)([A-Z])", r"\1 \2",regex=True)

# df['Application']=df['Component'].str.split(':').str[0]
# df['Component']=df['Component'].str.split(':').str[-1]

# df.drop(columns=['Alert'],inplace=True)
# df = df[['Health Factor','Application','Component','Effort','Technology','Rule']]

# from os.path import abspath

# apps_df = df['Application'].drop_duplicates().to_frame()
# file_name = abspath(f'E:/work/WellsFargo/tech-debt-HamiltonCliona.xlsx')
# writer = ExcelWriter(file_name, engine='xlsxwriter')
# app_tab = format_table(writer,apps_df,'applications')
# writer.close

