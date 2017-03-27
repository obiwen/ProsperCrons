'''FetchMapInfo.py: a script for saving the map statistics from EVE Online XML API'''

from os import path #TODO: path->plumbum?

import requests
import pandas
import xml.etree.ElementTree as ET
import eveapi
from plumbum import cli

import prosper.common.prosper_logging as p_log
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
KILLS_FIELDS = config.get(ME, 'kills_fields')
JUMPS_FIELDS = config.get(ME, 'jumps_fields')
FAC_FIELDS = config.get(ME, 'fac_war_systems_fields')
SOV_FIELDS = config.get(ME, 'sovereignty_fields')


def convert_to_panda(
        data,
        fields
):
    return pandas.DataFrame(refactor_to_docs(data, fields))

def refactor_to_docs(
        root,
        fields):

    for child in root.solarSystems:
        row = dict()
        for field in fields:
            row[field] = child.get(field)
        yield row


class XmlDriver(cli.Application):

    def main(self):
        '''meat of script.  Logic runs here.  Write like step list'''
        logger.info('Getting Map info')

        eveapi.set_user_agent("eveapi.py/1.3")

        api = eveapi.EVEAPIConnection()

        kills = api.map.kills()
        jumps = api.map.Jumps()
        facWarSystems = api.map.FacWarSystems()
        sovereignty = api.map.Sovereignty()

        killsDataframe = convert_to_panda(kills, KILLS_FIELDS.split(','))
        jumpsDataframe = convert_to_panda(jumps, JUMPS_FIELDS.split(','))
        facWarSystemsDataframe = convert_to_panda(facWarSystems, FAC_FIELDS.split(','))
        sovereigntyDataframe = convert_to_panda(sovereignty, SOV_FIELDS.split(','))

        logger.info('Saving to CSV File')
        killsDataframe.to_csv("kills.csv")
        jumpsDataframe.to_csv("jumps.csv")
        facWarSystemsDataframe.to_csv("facWarSystems.csv")
        sovereigntyDataframe.to_csv("sovereignty.csv")



if __name__ == '__main__':
    XmlDriver.run()
