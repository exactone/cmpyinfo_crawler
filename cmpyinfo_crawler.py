from urllib.parse import urlencode
import requests
from bs4 import BeautifulSoup
from lxml import etree
import time
import re

import sys

class CmpyinfoCrawlerError(Exception):
    def __init__(self, prefix, sta=0):
        self.sta = sta
        self.end = time.time()
        self.exe = self.end - self.sta
        self.prefix = prefix
            
    def __str__(self):
        # 'Response200Error'
        # 'QueryTypeError'
        s = self.prefix + " raised at {} ".format(self.timestr(self.exe))
        return s
    
    def timestr(self, t):
        d = int(t/86400)
        t = t%86400
        h = int(t/3600)    
        t = t%3600
        m = int(t/60)
        t = t%60
        s = t
        return "{} days {:02}:{:02}:{:.2f}".format(d, h, m, s)


class cmpyinfo_crawler:
    user_agent_list = ['Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
                       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
                       'Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6',
                       'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6',
                       'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5',
                       'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
                       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
                       'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24',
                       'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24'
                      ]
    
    proxies = [{'http':'http://proxy.hinet.net:80'}, {'http':'172.103.3.156:53281'}]
    
    h3_dict = {
        'HC' : '公司基本資料',
        'HB' : '商業登記基本資料',
        'HF' : '工廠基本資料',
        'BC' : '分公司資料',
        'HL' : '有限合夥',
        'BL' : '有限合夥登記基本資料(分支機構)'
    }
    type_dict = {
        'HC' : '公司',
        'HB' : '商號',
        'HF' : '工廠',
        'BC' : '分公司',
        'HL' : '有限合夥',
        'BL' : '有限合夥分支'
    }
    url2_dict = {
        'HC' : 'http://findbiz.nat.gov.tw/fts/query/QueryCmpyDetail/queryCmpyDetail.do',
        'HB' : 'http://findbiz.nat.gov.tw/fts/query/QueryBusmDetail/queryBusmDetail.do',
        'HF' : 'http://findbiz.nat.gov.tw/fts/query/QueryFactDetail/queryFactDetail.do',
        'BC' : 'http://findbiz.nat.gov.tw/fts/query/QueryBrCmpyDetail/queryBrCmpyDetail.do',
        'HL' : 'http://findbiz.nat.gov.tw/fts/query/QueryLmtdDetail/queryLmtdDetail.do',
        'BL' : 'http://findbiz.nat.gov.tw/fts/query/QueryLmtdDetail/queryLmtdDetail.do'
    }
    cmpy_type_dict = {
        'HC' : 'Cmpy',
        'HB' : 'Busm',
        'HF' : 'Fact',
        'BC' : 'BrCmpy',
        'HL' : 'Lmtd',
        'BL' : 'BrLmtd'
    }
    
    def __init__(self, iqryCond):
        self.session = None
        self.qryCond = iqryCond
        # self.banKey = None
        # self.objectId = None
        
        self.banNo = None
        self.brBanNo = None
        self.banKey = None
        self.estbId = None
        self.objectId = None
        
        self.querytype = None
        self.cmpy_type = None
        
        self.response = None
        self.h3_text = None
        self.sta = time.time()
        self.end = time.time()
    
    
    def random_pick_agent(self):
        import random
        return random.choice(cmpyinfo_crawler.user_agent_list)

    def get_banKey_objectId(self, attri):
        self.objectId = (attri.replace("javascript:qryDetail('","")).replace("', true);return false;","")
        self.querytype = self.objectId[0:2]
        self.cmpy_type = cmpyinfo_crawler.cmpy_type_dict[self.querytype]
        
        try:
            if self.querytype not in cmpyinfo_crawler.url2_dict:
                raise CmpyinfoCrawlerError('QueryTypeError', self.sta)
        except CmpyinfoCrawlerError as ccerr:
            print(ccerr)
            return       
                  
        #banKey = objectId[objectId.rindex(qryCond) + len(qryCond):]
        #print('objectId:', objectId)
        if self.querytype == 'HC': # 公司
            self.banNo = self.objectId.replace('HC', '')
            self.brBanNo = ''
            self.banKey = ''
            self.estbId = ''
        elif self.querytype == 'BC': # 分公司
            self.banNo = ''
            self.brBanNo = self.objectId.replace('BC', '')
            self.banKey = ''
            self.estbId = ''
        elif self.querytype == 'HB': # 商號
            self.banNo = self.qryCond
            self.brBanNo = ''
            self.banKey = self.objectId[self.objectId.rindex(self.qryCond) + len(self.qryCond):]
            self.estbId = ''
        elif self.querytype == 'HF': # 工廠
            self.banNo = ''
            self.brBanNo = ''
            self.banKey = ''
            self.estbId = self.objectId.replace('HF', '')
        elif self.querytype == 'HL': # 有限合夥
            self.banNo = self.objectId.replace('HL', '')
            self.brBanNo = ''
            self.banKey = ''
            self.estbId = ''
        elif self.querytype == 'BL': # 有限合夥分支
            self.banNo = self.objectId.replace('BL', '')
            self.brBanNo = ''
            self.banKey = ''
            self.estbId = ''

    
        print('banNo:', self.banNo, 'brBanNo:', self.brBanNo, 'banKey:', self.banKey, 'estbId:', self.estbId)
        
        
    def first_connection(self):
        url1 = 'http://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do'
        form_data_url1 = {
            'qryCond':str(self.qryCond),
            'infoType':'D',
            'qryType':'cmpyType',
            'cmpyType':'true',
            'qryType':'brCmpyType',
            'brCmpyType':'true',
            'qryType':'busmType',
            'busmType':'true',
            'qryType':'factType',
            'factType':'true',
            'qryType':'lmtdType',
            'lmtdType':'true',
            'isAlive':'all'
        }

        request_header1 = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language':'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4',
            'Cache-Control':'max-age=0',
            'Connection':'keep-alive',
            ##'Content-Length':'239',
            ##'Content-Type':'application/x-www-form-urlencoded',
            ##'Cookie':'qryCond=00114003~type=cmpyType,brCmpyType,busmType,factType,lmtdType,~infoType=D~isAlive=all~; JSESSIONID=6D2FA44A8850268F8A6A9D429D53B548; DWRSESSIONID=*o94X5xiFwhD8b5kRGWQo7XpKTl; JSESSIONID=33352345177332F62F210E99D78BD44A; _ga=GA1.3.315276753.1502943360; _gid=GA1.3.754870603.1502943360; _gat=1',
            ##'Host':'findbiz.nat.gov.tw',
            'Origin':'http://findbiz.nat.gov.tw',
            'Referer':'http://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do',
            'Upgrade-Insecure-Requests':'1',
            #'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'   
        }
        request_header1['User-Agent'] = self.random_pick_agent()

        self.session = requests.Session()
        self.response = self.session.post(url1, headers=request_header1,data=form_data_url1)
        
        try:
            if self.response.status_code != 200:
                raise CmpyinfoCrawlerError('Response200Error', self.sta)
        except CmpyinfoCrawlerError as ccerr:
            print(ccerr)
            return
            
        selector = etree.HTML(self.response.content)
        a = selector.xpath('//*[@id="vParagraph"]/div[@class="panel panel-default"]/div[@class="panel-heading companyName"]/a')
        self.get_banKey_objectId(a[0].attrib['oncontextmenu'])
        
        
    def second_connection(self):
        url2 = cmpyinfo_crawler.url2_dict[self.querytype]
        
        form_data_url2 = {
            'banNo':str(self.banNo),
            'brBanNo':str(self.brBanNo),
            'banKey':str(self.banKey),
            'estbId':str(self.estbId),
            'objectId':str(self.objectId),
            'CPage':'',
            'brCmpyPage':'',
            'eng':'',
        }

        request_header2={
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language':'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4',
            'Cache-Control':'max-age=0',
            'Connection':'keep-alive',
            ##'Content-Length':'211',
            ##'Content-Type':'application/x-www-form-urlencoded',
            ##'Cookie':'JSESSIONID=3193CF8D61AEC43F3810BF07524701BA; DWRSESSIONID=*o94X5xiFwhD8b5kRGWQo7XpKTl; _gat=1; _ga=GA1.3.315276753.1502943360; _gid=GA1.3.754870603.1502943360',
            ##'Host':'findbiz.nat.gov.tw',
            'Origin':'http://findbiz.nat.gov.tw',
            'Referer':'http://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do',
            ##'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        }
    
        request_header2['User-Agent'] = self.random_pick_agent()
        # global session
        # global res
        self.response = self.session.post(url2, headers=request_header2,data=form_data_url2)
            
        try:
            if self.response.status_code != 200:
                raise CmpyinfoCrawlerError('Response200Error', self.sta)
        except CmpyinfoCrawlerError as ccerr:
            print(ccerr)
            return

        return
        
    def get_h3(self):
        selector = etree.HTML(self.response.content)
        h3 = selector.xpath('//div[@id="content"]/div[@class="tab-content"]/div[@class="tab-pane active"]/h3')
        self.h3_text = h3[0].text.encode('latin_1', errors='ignore').decode('utf8', errors='ignore')
        
        try:
            if self.h3_text not in cmpyinfo_crawler.type_dict.values():
                raise CmpyinfoCrawlerError('h3ResolveError', self.sta)
        except CmpyinfoCrawlerError as ccerr:
            print(ccerr)
            return
        
    def parse_and_gen_schema(self):
        self.first_connection()
        self.second_connection()
        self.get_h3()
        parser = parser_cmpy_type(self.cmpy_type, self.qryCond)
        parser.parser(self.response)
        return parser.data_schema

class parser_cmpy_type:
    table_to_db_Cmpy={
        '統一編號':'banno',
        '公司狀況':'status',
        '公司名稱':'shop_name',
        '資本總額(元)':'capital',
        '實收資本額(元)':'null',
        '代表人姓名':'principal',
        '公司所在地':'address',
        '登記機關':'agency',
        '核准設立日期':'setupdate',
        '最後核准變更日期':'null',
        '所營事業資料':'businesslist'
    }
    
    table_to_db_BrCmpy={
        '分公司統一編號':'brbanno',
        '分公司狀況':'status',
        '分公司名稱':'shop_name',
        '分公司經理姓名':'principal',
        '分公司所在地':'address',
        '核准設立日期':'setupdate',
        '最後核准變更日期':'null',
        '總(本)公司統一編號':'banno',
        '總(本)公司名稱':'null'
    }

    table_to_db_Busm={
        '登記機關':'agency',
        '商業統一編號':'banno',
        '核准設立日期':'setupdate',
        '最近異動日期':'null',
        '商業名稱':'shop_name',
        '負責人姓名':'principal',
        '現況':'status',
        '資本額(元)':'capital',
        '組織類型':'orgtype',
        '地址':'address',
        '營業項目':'businesslist'
    }

    table_to_db_Fact={
        '登記機關':'agency',
        '工廠名稱':'shop_name',
        '工廠登記編號':'register_i',
        '工廠登記核准日期':'null',
        '工廠設立許可案號':'estbid',
        '工廠設立核准日期':'setupdate',
        '工廠地址':'address',
        '工廠負責人姓名':'principal',
        '公司(營利事業)統一編號':'null',
        '工廠組織型態':'null',
        '工廠資本額':'capital',
        '工廠登記狀態':'status',
        '最後核准變更日期':'null',
        '工廠設立許可廢止核准日期':'null',
        '工廠登記歇業核准日期':'null',
        '工廠登記廢止核准日期':'null',
        '工廠登記公告廢止核准日期':'null',
        '最近一次校正年度':'null',
        '最近一次校正結果':'null',
        '依據行政院主計處『中華民國行業標準分類』':'null',
        '產業類別':'businesslist',
        '主要產品':'null'
    }

    table_to_db_Lmtd={
        '登記機關':'agency',
        '統一編號':'banno',
        '有限合夥名稱':'shop_name',
        '所在地':'address',
        '實收出資額(元)':'capital',
        '核准設立日期':'setupdate',
        '現況':'status',
        '存續期間':'null',
        '最近一次登記狀況':'null',
        '代表人姓名':'principal',
        '普通合夥人姓名':'null',
        '有限合夥人':'null',
        '經理人姓名':'null',
        '約定解散事由':'null',
        '所營事業項目':'businesslist'
    }
    
    table_to_db_BrLmtd={
        '登記機關':'agency',
        '統一編號':'brbanno',
        '分支機構名稱':'shop_name',
        '所在地':'address',
        '在中華民國境內營運資金':'capital',
        '核准設立日期':'setupdate',
        '登記狀況':'status',
        '最近一次登記狀況':'null',
        '在中華民國境內負責人':'principal',
        '分支機構經理人':'null',
        '所營事業項目':'businesslist',
        '本機構統一編號':'banno',
        '本機構名稱':'null'
    }
    
    table_to_db_assign = {
        'Cmpy':table_to_db_Cmpy,
        'Busm':table_to_db_Busm,
        'Fact':table_to_db_Fact,
        'BrCmpy':table_to_db_BrCmpy,
        'Lmtd':table_to_db_Lmtd,
        'BrLmtd':table_to_db_BrLmtd        
    }
    
    rule_Cmpy={
        '統一編號':'rule0',
        '公司狀況':'rule0',
        '公司名稱':'rule0',
        '資本總額(元)':'rule4',
        '實收資本額(元)':'null',
        '代表人姓名':'rule0',
        '公司所在地':'rule0',
        '登記機關':'rule0',
        '核准設立日期':'rule3',
        '最後核准變更日期':'null',
        '所營事業資料':'rule5'
    }
    
    rule_BrCmpy={
        '分公司統一編號':'rule0',
        '分公司狀況':'rule0',
        '分公司名稱':'rule0',
        '分公司經理姓名':'rule0',
        '分公司所在地':'rule0',
        '核准設立日期':'rule3',
        '最後核准變更日期':'null',
        '總本公司統一編號':'rule1',
        '總本公司名稱':'null'
    }

    rule_Busm={
        '登記機關':'rule0',
        '商業統一編號':'rule0',
        '核准設立日期':'rule3',
        '最近異動日期':'null',
        '商業名稱':'rule0',
        '負責人姓名':'rule2',
        '現況':'rule0',
        '資本額(元)':'rule4',
        '組織類型':'rule0',
        '地址':'rule0',
        '營業項目':'rule5'
    }

    rule_Fact={
        '登記機關':'rule0',
        '工廠名稱':'rule0',
        '工廠登記編號':'rule0',
        '工廠登記核准日期':'null',
        '工廠設立許可案號':'rule0',
        '工廠設立核准日期':'rule3',
        '工廠地址':'rule0',
        '工廠負責人姓名':'rule0',
        '公司(營利事業)統一編號':'null',
        '工廠組織型態':'null',
        '工廠資本額':'rule4',
        '工廠登記狀態':'rule0',
        '最後核准變更日期':'null',
        '工廠設立許可廢止核准日期':'null',
        '工廠登記歇業核准日期':'null',
        '工廠登記廢止核准日期':'null',
        '工廠登記公告廢止核准日期':'null',
        '最近一次校正年度':'null',
        '最近一次校正結果':'null',
        '依據行政院主計處『中華民國行業標準分類』':'null',
        '產業類別':'rule5',
        '主要產品':'null'
    }

    rule_Lmtd={
        '登記機關':'rule0',
        '統一編號':'rule0',
        '有限合夥名稱':'rule0',
        '所在地':'rule0',
        '實收出資額(元)':'rule4',
        '核准設立日期':'rule3',
        '現況':'rule0',
        '存續期間':'null',
        '最近一次登記狀況':'null',
        '代表人姓名':'rule2',
        '普通合夥人姓名':'null',
        '有限合夥人':'null',
        '經理人姓名':'null',
        '約定解散事由':'null',
        '所營事業項目':'rule5'
    }
    
    rule_BrLmtd={
        '登記機關':'rule0',
        '統一編號':'rule0',
        '分支機構名稱':'rule0',
        '所在地':'rule0',
        '在中華民國境內營運資金':'rule4',
        '核准設立日期':'rule3',
        '登記狀況':'rule0',
        '最近一次登記狀況':'null',
        '在中華民國境內負責人':'rule0',
        '分支機構經理人':'null',
        '所營事業項目':'rule5',
        '本機構統一編號':'rule0',
        '本機構名稱':'null'
    }
    
    rule_assign = {
        'Cmpy':rule_Cmpy,
        'Busm':rule_Busm,
        'Fact':rule_Fact,
        'BrCmpy':rule_BrCmpy,
        'Lmtd':rule_Lmtd,
        'BrLmtd':rule_BrLmtd        
    }
    
    cmpy_type_to_type = {
        'Cmpy':'公司',
        'Busm':'商號',
        'Fact':'工廠',
        'BrCmpy':'分公司',
        'Lmtd':'有限合夥',
        'BrLmtd':'有限合夥分公司'          
    }
    
    def __init__(self, cmpy_type, qryCond):
        self.table_to_db = parser_cmpy_type.table_to_db_assign[cmpy_type]
        self.tr_rule = parser_cmpy_type.rule_assign[cmpy_type]              

        self.data_schema={
            'uniform_nu':str(qryCond), # varchar
            'register_i':None, # varchar
            'principal':None, # varchar
            'agency':None, # varchar
            'status':None, # varchar
            'address':None, # varchar
            'type':parser_cmpy_type.cmpy_type_to_type[cmpy_type], # varchar
            'shop_name':None, # varchar
            'update_tim':None, # varchar
            'banno':None, # varchar
            'brbanno':None, # varchar
            'bankey':None, # varchar
            'estbid':None, # varchar
            'objectid':None, # varchar
            'querytype':str(cmpy_type), # varchar
            'latitude':0, # double
            'longtitude':0, # double
            'capital':None, # bigint
            'orgtype':None, # varchar
            'mainbusiness':None, # varchar
            'businesslist':None, # longtext
            'setupdate':None,# date
        }
        
        self.td_rule_handler = {
            'rule0':self.ordinary_rule,
            'rule1':self.special_rule1,
            'rule2':self.special_rule2,
            'rule3':self.special_rule3,
            'rule4':self.special_rule4,
            'rule5':self.special_rule5,
        }
        
    def clean_text(self, text):
        import re
        if text is None:
            text = ''
            return text
        
        text = text.encode('latin_1', errors='ignore').decode('utf8', errors='ignore')
        text = re.sub(r'\s', r'', text)
        text = re.sub(r'[「」]', r'', text)
        return text
    
    def clean_Minguo_calendar(self, text):
        if text is None:
            text = ''
            return text
            
        text_year = text.split('年')[0]
        text_month = text.split('年')[1].split('月')[0]
        text_day = text.split('年')[1].split('月')[1].replace('日','')
        text_year = str(int(text_year) + 1911)
        return "{}-{}-{}".format(text_year, text_month, text_day)
    
    def special_rule1(self, td):
        """
        分公司資料
          - 總(本)公司統一編號
          - 總(本)公司名稱
        <td>
            <a href="#" onclick="javascript:queryCmpy('03557311', false)" oncontextmenu="javascript:queryCmpy('03557311', true);return false;">
                03557311
            </a>
        </td>
        """        
        a = td.xpath('./a[@href="#"]')
        return self.clean_text(a.text)
        
    def special_rule2(self, td):
        """
        商業登記基本資料
          - 負責人姓名
        <td>
            <table style="width:100%">
                <tbody><tr>
                    <td width="50%">黃霈雯</td>
                    <td width="50%">出資額(元):3,000</td>
                </tr></tbody>
            </table>
        </td>
        """
        tdtds = td.xpath('./table/tr/td')
        return self.clean_text(tdtds[0].text)
        
    def special_rule3(self, td):
        """
        公司基本資料
          - 核准設立日期
          - 最後核准變更日期
        分公司資料
          - 核准設立日期
          - 最後核准變更日期
        商業登記基本資料
          - 核准設立日期
          - 最近異動日期
        工廠基本資料
          - 工廠登記核准日期
          - 最後核准變更日期
        <td>105年07月28日</td>
        """
        text = self.clean_text(td.text)
        text = re.sub(r'\(.*\)',r'', text)
        return self.clean_Minguo_calendar(text)
        
        
    def special_rule4(self, td):
        """
        公司基本資料
          - 資本總額(元)
        商業登記基本資料
          - 資本額(元)
        <td>
            3,000
        </td>
        """
        text = self.clean_text(td.text).replace(',', '')
        if not text.isdigit():
            text = ''
        return text
        
    def special_rule5(self, td):
        """
        公司基本資料
          - 所營事業資料
        
        <td>
            "F106010&nbsp;
            五金批發業"
            <br>
            "F113010&nbsp;
            機械批發業"
            <br>
            "F113020&nbsp;
            電器批發業"
            <br>
            ...
            "ZZ99999&nbsp;
            除許可業務外，得經營法令非禁止或限制之業務"
            <br>
        </td> 
        """
        tds = td.xpath('./td')
        text = list()
        for t in td.xpath('./text()'):
            t = t.encode('latin_1', errors='ignore').decode('utf8', errors='ignore')
            t = re.sub(r'\s', r'', t)
            t = re.sub(r'[0-9A-Z一二三四五六七八九\.]', r'',  t)
            if t:
                text.append(t)
            
        return text
        
    def ordinary_rule(self, td):
        return self.clean_text(td.text)
    
    def parser(self, res):
        selector = etree.HTML(res.content)
        trs = selector.xpath('//div[@class="table-responsive"]/table[@class="table table-striped"]/tbody/tr')
        for tr in trs:
            tds = tr.xpath('./td')
            td0 = tds[0]
            key = self.ordinary_rule(td0)
            
            
            if key in self.tr_rule: 
                td1 = tds[1]
                
                if self.tr_rule[key] != 'null':
                    rule = self.tr_rule[key]
                    value = self.td_rule_handler[rule](td1)
                    self.data_schema[self.table_to_db[key]] = value
        else:
            self.data_schema['mainbusiness'] = self.data_schema['businesslist'][0] if len(self.data_schema['businesslist']) else ""
            