import json
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def json_datastet(filename):
    try:
        p = json.load(open(filename, 'r'))
        return parse_datastet(p)
    except:
        logger.debug(f'error with datastet {filename}')
        return {}

def parse_datastet(p):
    details = {'mentions': [], 
                        'used': [], 'created': [], 'shared': [],
                        'has_used': False, 'has_created': False, 'has_shared': False,
                        'nb_used': 0, 'nb_created': 0, 'nb_shared': 0
                       }
    res = {'datastet_details': details}
    for m in p['mentions']:
        current_mention = {}
        if m.get('wikidataId'):
            current_mention['wikidata'] = m['wikidataId']
        name = m['dataset-implicit']['normalizedForm']
        current_mention['name'] = name
        #for mention_type in ['used', 'created', 'shared']:
        #    current_mention[mention_type] = m['documentContextAttributes'][mention_type]['value']
        #    if current_mention[mention_type] and name not in details[mention_type]:
        #        details[mention_type].append(name)
        #        details[f'has_{mention_type}'] = True
        #        details[f'nb_{mention_type}'] += 1
        details['mentions'].append(current_mention)
    
    return res