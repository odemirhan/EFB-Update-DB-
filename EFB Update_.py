# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 16:57:51 2021

@author: Engineering
"""


import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import pyodbc

payload = {'j_username' : 'xxxx',
          'j_password' : 'xxxx'}

now=datetime.now()
nowm3=now+timedelta(hours=-3)

 
conn36=pyodbc.connect('Driver={SQL Server};'
                              'Server=localhost;'
                              'Database=FuelMasterDB;'
                              'Trusted_Connection=yes;'
                              )


with requests.Session() as session:
    session.keep_alive=False
    

    r = session.post('https://jdmp-rest.jeppesen.com/restapi/j_spring_security_check', data=payload, headers={'Connection':'close'})
    
    sessionid=r.cookies
    
    
    
    a=session.get('https://jdmp-rest.jeppesen.com/restapi/recipient',cookies=sessionid)
    time.sleep(5)
    b=session.get('https://jdmp-rest.jeppesen.com/restapi/DeviceInfo',cookies=sessionid)
    time.sleep(5)
    c=session.get('https://jdmp-rest.jeppesen.com/restapi/ContentItem',cookies=sessionid)
    
    
    
    j = a.json()
    
    
    df = pd.DataFrame.from_dict(j)
    
    
    df = pd.json_normalize(df["pageData"])
    
    k=b.json()
    df2 = pd.DataFrame.from_dict(k)
    
    
    df2 = pd.json_normalize(df2["pageData"])
    
    l=c.json()
    df3 = pd.DataFrame.from_dict(l)
    
    
    df3 = pd.json_normalize(df3["pageData"])
    
    df3['lastupdatetime']=pd.to_datetime(df3["contentItem.lastVersionPublished"], format='%Y-%m-%dT%H:%M:%SZ')
    df3["associatedApplications"]=df3["associatedApplications"].astype('str')
    OPTcontentDF=df3[df3["associatedApplications"]=="['OPT']"]
    OPTcontentDT=OPTcontentDF['lastupdatetime'].max()
    JEPcontentDF=df3[df3["associatedApplications"]!="['OPT']"]
    JEPcontentDT=JEPcontentDF['lastupdatetime'].max()
    
    RecipientDF=pd.merge(left=df, right=df2, left_on='id', right_on='recipient.id')

with requests.Session() as session:
    	
    miradoreses=session.get('https://corendonairlines.online.miradore.com/API/Devicexxxx&select=Tag.Name,User.Email,User.Lastname&filters=User.Email notisempty&options=rows=1000,page=1')

DF=pd.DataFrame([], columns=["Email", "Tag", "TLC"])
root = ET.fromstring(miradoreses.content)
for Device in root.iter('Device'):
    for User in Device.findall('User'):
        #for UserEmail in User.findall('Email'):
        dumUserEmail=User.find('Email').text
        try:
            dumUserLN=User.find('Lastname').text
        except: 
            dumUserLN=""
        
    for Tags in Device.findall('Tags'):
        for Tag in Tags.findall('Tag'):
        
        
        #for TagName in Tags.findall('Name'):
            dumTagName=Tag.find('Name').text
                
            dummyDF=pd.DataFrame([[dumUserEmail, dumTagName, dumUserLN]], columns=["Email", "Tag", "TLC"])
            DF=DF.append(dummyDF)

MergedDF=pd.merge(left=DF, right=RecipientDF, left_on='Tag', right_on='recipientName')

MergedDF=MergedDF[["Email", "Tag", "TLC", "applicationName", "applicationVersion", "lastUpdated_y"]]
MergedDF['lastUpdated_y']=pd.to_datetime(MergedDF["lastUpdated_y"], format='%Y-%m-%dT%H:%M:%SZ')
MergedDF["key"]=MergedDF["Tag"]+MergedDF["TLC"]+MergedDF["applicationName"]

dummyDF=MergedDF.groupby("key").max()

SCHdf=pd.read_csv(r'C:\Users\Engineering\TURISTIK HAVA TASIMACILIK A.S\coreSafety - VERSION_03/SCH_dynamic.csv')

SCHdf=SCHdf[["ACREG","CARRIER","FNO","SDEP","SARR","STD", "C1", "C2"]]
SCHdf['C1']=SCHdf['C1'].str.strip()
SCHdf['C2']=SCHdf['C2'].str.strip()
SCHdf["FNO"]=SCHdf["FNO"].astype('str')
#SCHdf["SCHkey"]=SCHdf["ACREG"]+SCHdf["CARRIER"]+SCHdf["FNO"].astype('str')+SCHdf["SDEP"]+SCHdf["SARR"]+SCHdf["STD"]
SCHdf["STD"]=pd.to_datetime(SCHdf["STD"], format='%Y-%m-%d %H:%M:%S')
SCHdf=SCHdf[(SCHdf["STD"]>nowm3) & (SCHdf["STD"]<now)]

dummyOPT=dummyDF[dummyDF["applicationName"]=='OPT']
dummyOPT["TLC"]=dummyOPT["TLC"].str.strip()
dummyJEP=dummyDF[dummyDF["applicationName"]=='FD Pro X']
dummyJEP["TLC"]=dummyJEP["TLC"].str.strip()

FinalDF=pd.merge(left=SCHdf, right=dummyOPT, left_on="C1", right_on="TLC")
#FinalDF['applicationVersion']=FinalDF['C1_OPT_Version']
FinalDF['C1_OPT_Last_Updated']=FinalDF['lastUpdated_y']
FinalDF['C1_Email']=FinalDF['Email']
FinalDF['C1_Tag']=FinalDF['Tag']
FinalDF=FinalDF[['ACREG', 'CARRIER', 'FNO', 'SDEP', 'SARR', 'STD', 'C1', 'C2','C1_Tag','C1_Email', 'C1_OPT_Last_Updated']]


FinalDF=pd.merge( FinalDF, dummyOPT, left_on="C2", right_on="TLC")
FinalDF['C2_OPT_Last_Updated']=FinalDF['lastUpdated_y']
FinalDF['C2_Email']=FinalDF['Email']
FinalDF['C2_Tag']=FinalDF['Tag']
FinalDF=FinalDF[['ACREG', 'CARRIER', 'FNO', 'SDEP', 'SARR', 'STD', 'C1','C1_Tag','C1_Email', 'C1_OPT_Last_Updated', 'C2', 'C2_Tag','C2_Email', 'C2_OPT_Last_Updated']]


FinalDF=pd.merge(FinalDF, dummyJEP, left_on="C1", right_on="TLC")
FinalDF['C1_JEP_Last_Updated']=FinalDF['lastUpdated_y']
FinalDF=FinalDF[['ACREG', 'CARRIER', 'FNO', 'SDEP', 'SARR', 'STD', 'C1','C1_Tag','C1_Email', 'C1_OPT_Last_Updated', 'C1_JEP_Last_Updated', 'C2', 'C2_Tag','C2_Email', 'C2_OPT_Last_Updated']]




FinalDF=pd.merge(FinalDF, dummyJEP, left_on="C2", right_on="TLC")
FinalDF['C2_JEP_Last_Updated']=FinalDF['lastUpdated_y']
FinalDF=FinalDF[['ACREG', 'CARRIER', 'FNO', 'SDEP', 'SARR', 'STD', 'C1','C1_Tag','C1_Email', 'C1_OPT_Last_Updated', 'C1_JEP_Last_Updated', 'C2', 'C2_Tag','C2_Email', 'C2_OPT_Last_Updated', 'C2_JEP_Last_Updated']]


FinalDF["C1_OPT_Updated"]=['YES' if x > OPTcontentDT else 'NO' for x  in FinalDF['C1_OPT_Last_Updated']]
FinalDF["C2_OPT_Updated"]=['YES' if x > OPTcontentDT else 'NO' for x  in FinalDF['C2_OPT_Last_Updated']]
FinalDF["C1_JEP_Updated"]=['YES' if x > JEPcontentDT else 'NO' for x  in FinalDF['C1_JEP_Last_Updated']]
FinalDF["C2_JEP_Updated"]=['YES' if x > JEPcontentDT else 'NO' for x  in FinalDF['C2_JEP_Last_Updated']]


FinalDF["OPT_LatestPackage"]=OPTcontentDT
FinalDF["JEP_LatestPackage"]=JEPcontentDT

FinalDF=FinalDF[['ACREG', 'CARRIER', 'FNO', 'SDEP', 'SARR', 'STD', 'C1','C1_Tag','C1_Email',"C1_OPT_Updated", 'C1_OPT_Last_Updated', "C1_JEP_Updated", 'C1_JEP_Last_Updated', 'C2', 'C2_Tag','C2_Email',"C2_OPT_Updated", 'C2_OPT_Last_Updated',"C2_JEP_Updated", 'C2_JEP_Last_Updated',"OPT_LatestPackage","JEP_LatestPackage"]]
   


for cnt4 in range(len(FinalDF.index)):
    
    
    dfaslist=FinalDF.iloc[cnt4].tolist()
    
    
        
    cur1=conn36.cursor()
    cur1.execute("SELECT * from dbo.[EFB Update] WHERE  Aircraft=?  AND Carrier=? AND [Flight No]=? AND Departure=? AND Arrival=? AND STD=?", [dfaslist[0], dfaslist[1], dfaslist[2], dfaslist[3], dfaslist[4], dfaslist[5]])
    ftcur=cur1.fetchone()
    
    if not ftcur:
       cur1.execute("""INSERT INTO dbo.[EFB Update] VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  dfaslist)
       conn36.commit()
    else: 
       cur1.execute("DELETE from dbo.[EFB Update] WHERE  Aircraft=?  AND Carrier=? AND [Flight No]=? AND Departure=? AND Arrival=? AND STD=?", [dfaslist[0], dfaslist[1], dfaslist[2], dfaslist[3], dfaslist[4], dfaslist[5]])
       cur1.execute("""INSERT INTO dbo.[EFB Update] VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  dfaslist)
       conn36.commit()