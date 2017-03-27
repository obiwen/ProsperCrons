'''FetchMapInfo.py: a script for saving the map statistics from EVE Online XML API'''

from os import path #TODO: path->plumbum?

import pandas
import eveapi
from plumbum import cli

import prosper.common.prosper_logging as p_logging
from prosper.common.prosper_config import get_config

HERE = path.abspath(path.dirname(__file__))
ME = __file__.replace('.py', '')
CONFIG_ABSPATH = path.join(HERE, 'cron_config.cfg')

CONFIG = get_config(CONFIG_ABSPATH)

## MAP INFO GLOBALS ##
KILLS_FIELDS = CONFIG.get(ME, 'kills_fields')
JUMPS_FIELDS = CONFIG.get(ME, 'jumps_fields')
FAC_FIELDS = CONFIG.get(ME, 'fac_war_systems_fields')
SOV_FIELDS = CONFIG.get(ME, 'sovereignty_fields')


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

    _log_builder = p_logging.ProsperLogger(
        ME,
        HERE
    )

    debug = cli.Flag(
        ['d', '--debug'],
        help='Debug mode, no production db, headless mode'
    )

    @cli.switch(
        ['-v', '--verbose'],
        help='Enable verbose messaging'
    )
    def enable_verbose(self):
        """toggle verbose logger"""
        self._log_builder.configure_debug_logger()


    def main(self):
        """Program Main flow"""
        global LOGGER
        if not self.debug:
            self._log_builder.configure_discord_logger()
        LOGGER = self._log_builder.logger

        LOGGER.info('Getting Map info')

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

        LOGGER.info('Saving to CSV File')
        killsDataframe.to_csv("kills.csv")
        jumpsDataframe.to_csv("jumps.csv")
        facWarSystemsDataframe.to_csv("facWarSystems.csv")
        sovereigntyDataframe.to_csv("sovereignty.csv")





if __name__ == '__main__':
    XmlDriver.run()
