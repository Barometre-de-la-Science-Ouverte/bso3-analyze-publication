import json
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def json_softcite(filename):
    try:
        p = json.load(open(filename, 'r'))
        return parse_softcite(p)
    except:
        logger.debug(f'error with softcite {filename}')
        return {}

def parse_softcite(softcite):
    softcite_details = {'mentions': [], 
                        'used': [], 'created': [], 'shared': [],
                        'has_used': False, 'has_created': False, 'has_shared': False,
                        'nb_used': 0, 'nb_created': 0, 'nb_shared': 0
                       }
    res = {'softcite_details': softcite_details}
    for m in softcite['mentions']:
        current_mention = {}
        if m.get('wikidataId'):
            current_mention['wikidata'] = m['wikidataId']
        name = m['software-name']['normalizedForm']
        current_mention['name'] = name
        for mention_type in ['used', 'created', 'shared']:
            current_mention[mention_type] = m['documentContextAttributes'][mention_type]['value']
            if current_mention[mention_type] and name not in softcite_details[mention_type]:
                softcite_details[mention_type].append(name)
                softcite_details[f'has_{mention_type}'] = True
                softcite_details[f'nb_{mention_type}'] += 1
        softcite_details['mentions'].append(current_mention)
    
    return res
