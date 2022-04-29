import time
import datetime
import os
import requests
import pandas as pd

from project.server.main.utils_swift import download_container, upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

volume = '/data'
container = 'bso3_publications_dump'

def create_task_analyze(args):
    for fileType in args.get('fileType', []):
        logger.debug(f'getting {fileType} data')
        if args.get('download', False):
            download_container(container, False, fileType, volume)
    if args.get('concat', False):
        df = read_all('softcite')
        df.to_json(f'{volume}/softcite.jsonl', orient='records', lines=True)
        upload_object('tmp', f'{volume}/softcite.jsonl', 'softcite.jsonl') 

def read_all(fileType):
    all_dfs = []
    ix = 0
    for root, dirs, files in os.walk(f'{volume}/{container}/{fileType}'):
        if files:
            for f in files:
                filename_softcite = f'{root}/{f}'
                root_metadata = root.replace(fileType, 'metadata')
                filename_metadata = f'{root_metadata}/{f}'.replace('.software.json', '.json.gz')
                try:
                    df_tmp_softcite = pd.read_json(filename_softcite, orient='records', lines=True)
                    df_tmp_softcite.columns = [f'softcite_{c}' for c in df_tmp_softcite.columns]
                    try:
                        df_tmp_metadata = pd.read_json(filename_metadata, orient='records', lines=True)
                        df_tmp_metadata.columns = [f'metadata_{c}' for c in df_tmp_metadata.columns]
                    except:
                        logger.debug(f'missing metadata {filename_metadata}')
                        download_container(container, False, '/'.join(filename_metadata.split('/')[3:-1]), volume)
                        df_tmp_metadata = pd.read_json(filename_metadata, orient='records', lines=True)
                        df_tmp_metadata.columns = [f'metadata_{c}' for c in df_tmp_metadata.columns]
                    df_tmp = pd.concat([df_tmp_softcite, df_tmp_metadata], axis=1)
                    all_dfs.append(df_tmp)
                except:
                    logger.debug(f'error in reading {filename_softcite}')
                ix += 1
                if ix % 1000 == 0:
                    logger.debug(f'{ix} files read')
    return pd.concat(all_dfs)
    
#    url_hal_update = "https://api.archives-ouvertes.fr/search/?fq=doiId_s:*%20AND%20structCountry_s:fr%20AND%20modifiedDate_tdate:[{0}T00:00:00Z%20TO%20{1}T00:00:00Z]%20AND%20producedDate_tdate:[2013-01-01T00:00:00Z%20TO%20{1}T00:00:00Z]&fl=halId_s,doiId_s,openAccess_bool&rows={2}&start={3}"

