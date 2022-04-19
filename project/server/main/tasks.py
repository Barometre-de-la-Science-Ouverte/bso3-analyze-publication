import time
import datetime
import os
import requests

from bso.server.main.utils_swift import download_container
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_analyze(arg):
    download_container('bso3_publications_dump', True, 'metadata', '/data'):
    
#    url_hal_update = "https://api.archives-ouvertes.fr/search/?fq=doiId_s:*%20AND%20structCountry_s:fr%20AND%20modifiedDate_tdate:[{0}T00:00:00Z%20TO%20{1}T00:00:00Z]%20AND%20producedDate_tdate:[2013-01-01T00:00:00Z%20TO%20{1}T00:00:00Z]&fl=halId_s,doiId_s,openAccess_bool&rows={2}&start={3}"

