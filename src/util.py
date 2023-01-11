from pandas import DataFrame,json_normalize,concat,ExcelWriter
from os import mkdir
from os.path import exists,abspath,join
from subprocess import Popen,PIPE,STDOUT
from logger import Logger
import sys


def get_between(txt,tag_start,tag_end,start_at=0):
    text = txt[start_at:]
    
    start = text[start_at:].find(f"{tag_start}")+len(f"{tag_start}")
    end = text[start:].find(f"{tag_end}")
    between = text[start:start+end].strip()

    return between,start,end,start-len(tag_start),end+len(tag_end)

def list_to_text(list):
    rslt = ""

    l = len(list)
    if l == 0:
        return ""
    elif l == 1:
        return list[0]

    data = list
    if l > 2:
        last_name = list[-2]
    else:
        last_name = ''

    for a in list:
        rslt = rslt + a + ", "

    rslt = rslt[:-2]
    rslt = rreplace(rslt,', ',' and ')

    return rslt

def rreplace(s, old, new, occurrence=1):
    li = s.rsplit(old, occurrence)
    return new.join(li)

def each_risk_factor(ppt, aip_data, app_id, app_no):
    # collect the high risk grades
    # if there are no high ris grades then use medium risk
    app_level_grades = aip_data.get_app_grades(app_id)
    risk_grades = aip_data.calc_health_grades_high_risk(app_level_grades)
    risk_catagory="high"
    if risk_grades.empty:
        risk_catagory="medium"
        risk_grades = aip_data.calc_health_grades_medium_risk(app_level_grades)
    
    #in the event all health risk factors are low risk
    if risk_grades.empty:
        ppt.replace_block(f'{{app{app_no}_risk_detail}}',
                          f'{{end_app{app_no}_risk_detail}}',
                          "no high-risk health factors")
    else: 
        ppt.replace_text(f'{{app{app_no}_risk_category}}',risk_catagory)
        ppt.copy_block(f'app{app_no}_each_risk_factor',["_risk_name","_risk_grade"],len(risk_grades.count(axis=1)))
        f=1
        for index, row in risk_grades.T.iteritems():
            ppt.replace_text(f'{{app{app_no}_risk_name{f}}}',index)
            ppt.replace_text(f'{{app{app_no}_risk_grade{f}}}',row['All'].round(2))
            f=f+1

        ppt.replace_text(f'{{app{app_no}_risk_detail}}','')
        ppt.replace_text(f'{{end_app{app_no}_risk_detail}}','')

    ppt.remove_empty_placeholders()
    return risk_grades

def format_table(writer, data, sheet_name,width=None):
    
    data.to_excel(writer, index=False, sheet_name=sheet_name, startrow=1,header=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    rows = len(data)
    cols = len(data.columns)-1
    columns=[]
    for col_num, value in enumerate(data.columns.values):
        columns.append({'header': value})

    table_options={
                'columns':columns,
                'header_row':True,
                'autofilter':True,
                'banded_rows':True
                }
    worksheet.add_table(0, 0, rows, cols,table_options)
    
    header_format = workbook.add_format({'text_wrap':True,
                                        'align': 'center'})

    col_width = 10
    if width == None:
        width = []
        for i in range(1,len(data.columns)+1):
           width.append(col_width)
    for col_num, value in enumerate(data.columns.values):
        worksheet.write(0, col_num, value, header_format)
        w=width[col_num]
        worksheet.set_column(col_num, col_num, w)
    return worksheet

def find_nth(string, substring, n):
   if (n == 1):
       return string.find(substring)
   else:
       return string.find(substring, find_nth(string, substring, n - 1) + 1)

def no_dups(string, separator,add_count=False):
    alist = list(string.split(separator))
    alist.sort()
    nlist = []
    clist = []
    for i in alist:
        if i not in nlist:
            nlist.append(i)
            clist.append(1)
        else:
            idx = nlist.index(i)
            clist[idx]=clist[idx]+1

    if add_count:
        for val in nlist:
            idx = nlist.index(val)
            cnt = clist[idx]
            
            cval=''
            if cnt > 1:
                cval = f'({cnt})'
            val = f'{val}{cval}'
            nlist[idx]=val

    string = separator.join(nlist)
    return string

def toExcel(file_name, tabs):
    writer = ExcelWriter(file_name, engine='xlsxwriter')
    for key in tabs:
        format_table(writer,tabs.get(key),key)
    writer.save()

def resource_path(relative_path):
    "get the absolute path to resource, works for dev and for PyInstaller"
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = abspath('.')
    return join(base_path, relative_path)

def create_folder(folder):
    if not exists(folder):
        mkdir(folder)

def run_process(args,wait=True,output=True) -> int:
#    process = Popen(args, stdout=PIPE, stderr=PIPE)
    process = Popen(args, stdout=PIPE, stderr=STDOUT,text=True)
    if wait:
        return check_process(process,output)
    else:
        return process

def check_process(process:Popen,output=True):
    ret = []
    while process.poll() is None:
        line = process.stdout.readline()
        line = line.lstrip("b'").rstrip('\n')
        if output == True and len(line.strip(' ')) > 0:
            print(line)
        ret.append(line)
    stdout, stderr = process.communicate()
    return process.returncode,ret
    

def format_table(writer, data, sheet_name,width=None):
    
    data.to_excel(writer, index=False, sheet_name=sheet_name, startrow=1,header=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    rows = len(data)
    cols = len(data.columns)-1
    columns=[]
    for col_num, value in enumerate(data.columns.values):
        columns.append({'header': value})

    table_options={
                'columns':columns,
                'header_row':True,
                'autofilter':True,
                'banded_rows':True
                }
    worksheet.add_table(0, 0, rows, cols,table_options)
    
    header_format = workbook.add_format({'text_wrap':True,
                                        'align': 'center'})

    col_width = 10
    if width == None:
        width = []
        for i in range(1,len(data.columns)+1):
           width.append(col_width)
    for col_num, value in enumerate(data.columns.values):
        worksheet.write(0, col_num, value, header_format)
        w=width[col_num]
        worksheet.set_column(col_num, col_num, w)
    return worksheet

def convert_LOC(total:int):
    unit = ''
    if 1000 <= total <= 1000000:
        unit = 'KLoc'
        total = int(total/1000)
    elif total > 1000000:
        unit = 'MLoc'
        total = round(total/1000000,1)
    return total,unit