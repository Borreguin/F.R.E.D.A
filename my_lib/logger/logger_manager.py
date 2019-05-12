# Created by Roberto Sanchez at 3/25/2019

import logging
from settings import initial_settings as init


def check():
    lg = init.LogDefaultConfig()
    lg.logger.setLevel(logging.ERROR)
    for i in range(1, 1000):
        lg.logger.error("A message using warning level" + str(i))
        lg.logger.warning("A message using warning level" + str(i))
        lg.logger.info("A message using warning level" + str(i))
        lg.logger.debug("A message using verbose level" + str(i))
    return True


if __name__ == '__main__':
    check()

