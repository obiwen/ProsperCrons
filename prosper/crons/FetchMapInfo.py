'''FetchMapInfo.py: a script for saving the map statistics from EVE Online XML API'''

from os import path #TODO: path->plumbum?

import requests
import pandas
import xml.etree.ElementTree as ET
from plumbum import cli

from prosper.common.prosper_logging import create_logger
from prosper.common.prosper_config import get_config

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')

config = get_config(CONFIG_ABSPATH)
logger = create_logger(
    'debug-FetchMapInfo',
    HERE,
    config
)

## MAP INFO GLOBALS ##
XML_BASE_URL = config.get(ME, 'XML_endpoint')
XML_FILES = config.get(ME, 'files')


def get_map_info_from_eve(
    file
):
    url = XML_BASE_URL + file
    try:
        request = requests.get(
            url
        )
    except Exception as err_msg:
        logger.error(
            'EXCEPTION: request failed' +
            '\r\texception={0}'.format(err_msg) +
            '\r\turl={0}'.format(url)
        )
        raise err_msg

    if request.status_code == requests.codes.ok:
        try:
            response = request.content
        except Exception as err_msg:
            logger.error(
                'EXCEPTION: unable to parse payload' +
                '\r\texception={0}'.format(err_msg) +
                '\r\turl={0}'.format(err_msg)
            )
            logger.debug(request.text)
    else:
        logger.error(
            'EXCEPTION: bad status code' +
            '\r\texception={0}'.format(request.status_code) +
            '\r\turl={0}'.format(url)
        )
        logger.debug(request.text)
        raise Exception('BAD STATUS CODE ' + str(request.status_code))

    return response


def convert_to_panda(
        data
):
    etree = ET.fromstring(data)
    return pandas.DataFrame(refactor_to_docs(etree))

def refactor_to_docs(
        root):
    fields = []
    for rowset in root.iter('rowset'):
        fields = rowset.get("columns").split(',')
        logger.debug("found columns: " + str(fields))

    for child in root.iter('row'):
        row = dict()
        for field in fields:
            row[field] = child.get(field)
        yield row


class XmlDriver(cli.Application):

    def main(self):
        '''meat of script.  Logic runs here.  Write like step list'''
        logger.info('Getting Map info')
        for file in XML_FILES.split(','):
            logger.info('Getting File: ' + file)
            fetch_file = get_map_info_from_eve(file)
            panda_dataframe= convert_to_panda(fetch_file)
            logger.info('Saving to CSV File: ' + file)
            panda_dataframe.to_csv(file + ".csv")

if __name__ == '__main__':
    XmlDriver.run()
