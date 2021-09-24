#!/usr/bin/python3
# -*- coding: utf-8 -*-
# from Alma_Apis_Interface import 
import json
import os
import re
from datetime import date, timedelta
import logging
import logs
import AlmaApi
import mail
import xml.etree.ElementTree as ET

SERVICE = 'Alma_UserStatCategories_Control'
api_key = os.getenv("PROD_NETWORK_CONF_API")
path_to_report = "%2Fshared%2FBordeaux%20NZ%2033PUDB_NETWORK%2Fprod%2FSCOOP%2FUtilisateurs%2FListe%20des%20codes%20statistiques%2FCodes%20stat"

ns = {'xmlns': 'urn:schemas-microsoft-com:xml-analysis:rowset' }



#On initialise le logger
logs.init_logs(os.getenv('LOGS_PATH'),SERVICE,'INFO')
logger = logging.getLogger(SERVICE)

#On va chercher les codes déclarés dans la table UserStatCategories
api = AlmaApi.AlmaRecords(api_key,'EU',SERVICE)
response, table = api.get_table('UserStatCategories')
code_list=[]
for row in table['row']:
    code_list.append(row['code'])
logger.debug(len(code_list))
# On vachercher tous les codes assiciés à des lecteurs actifs dans Alma 
# Rapport 	/shared/Bordeaux NZ 33PUDB_NETWORK/prod/SCOOP/Utilisateurs/Liste des codes statistiques/Codes stat
# IsFinished
isFinished = 'false'
token = ''
cpteur = 0
while isFinished == 'false' :
    status, code_stats = api.get_stat(path_to_report,limit=25,token=token)
    # logger.debug(code_stats)
    reponsexml = ET.fromstring(code_stats)
    if reponsexml.findall(".//QueryResult/ResumptionToken") :
        token=reponsexml.find(".//QueryResult/ResumptionToken").text
    isFinished=reponsexml.find(".//QueryResult/IsFinished").text
    logger.debug(token)
    logger.debug(isFinished)
    rows = reponsexml.findall(".//QueryResult/ResultXml/xmlns:rowset/xmlns:Row",ns)
    
    for row in rows :
        code = row.find("./xmlns:Column1",ns).text
        exemple = row.find("./xmlns:Column2",ns).text
        if code not in code_list :
            cpteur = cpteur + 1
            logger.info("{}::{}".format(code,exemple))
logger.info(cpteur)
    