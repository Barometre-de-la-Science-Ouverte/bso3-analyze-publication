import time
import datetime
import os
import requests
import pandas as pd

from urllib import parse
from project.server.main.utils_swift import download_container, upload_object
from project.server.main.parse_grobid import json_grobid
from project.server.main.parse_datastet import json_datastet
from project.server.main.parse_softcite import json_softcite
from project.server.main.logger import get_logger

logger = get_logger(__name__)

volume = '/data'
container = 'bso3_publications_dump'

def create_task_analyze(args):
    prefix_uid = args.get('prefix_uid', '')
    logger.debug(f'analyze for prefix {prefix_uid}')
    GROBID_VERSIONS = args.get('GROBID_VERSIONS', [])
    SOFTCITE_VERSIONS = args.get('SOFTCITE_VERSIONS', [])
    DATASTET_VERSIONS = args.get('DATASTET_VERSIONS', [])
    if args.get('download', False):
        for fileType in ['metadata', 'grobid', 'softcite', 'datastet']:
            logger.debug(f'getting {fileType} data')
            download_container(container, f'{fileType}/{prefix_uid}', volume)
    read_all(prefix_uid, GROBID_VERSIONS, SOFTCITE_VERSIONS, DATASTET_VERSIONS)

def read_all(prefix_uid, GROBID_VERSIONS, SOFTCITE_VERSIONS, DATASTET_VERSIONS):
    ix = 0
    all_data = []
    for root, dirs, files in os.walk(f'{volume}/{container}/metadata/{prefix_uid}'):
        if files:
            for f in files:
                metadata_filename = f'{root}/{f}'
                uid = f.replace('.json.gz', '')
                #logger.debug(f'parsing {uid}')
                grobid_filename   = root.replace('metadata', 'grobid')   + '/' + f.replace('.json.gz', '.pdf.tei.xml')
                softcite_filename = root.replace('metadata', 'softcite') + '/' + f.replace('.json.gz', '.software.json')
                datastet_filename = root.replace('metadata', 'datastet') + '/' + f.replace('.json.gz', '.dataset.json')
                try:
                    df_metadata = pd.read_json(metadata_filename, lines=True, orient='records')[['doi', 'id']]
                    df_metadata.columns = ['doi', 'uid']
                    res = df_metadata.to_dict(orient='records')[0]
                    res['sources'] = ['bso3']
                except:
                    logger.debug(f'error with metadata {metadata_filename}')
                    continue
                if os.path.exists(grobid_filename):
                    res.update(json_grobid(grobid_filename, GROBID_VERSIONS))
                if os.path.exists(softcite_filename):
                    res.update(json_softcite(softcite_filename, SOFTCITE_VERSIONS))
                if os.path.exists(datastet_filename):
                    res.update(json_datastet(datastet_filename, DATASTET_VERSIONS))
                ix += 1
                if res.get('authors') or res.get('softcite_details') or res.get('datastet_details'):
                    all_data.append(res)
                if ix % 1000 == 0:
                    logger.debug(f'{ix} files read')
    result_filename = f'bso3_data_{prefix_uid}.jsonl'
    pd.DataFrame(all_data).to_json(result_filename, lines=True, orient='records')
    upload_object(container, result_filename, f'final_for_bso/{result_filename}')

