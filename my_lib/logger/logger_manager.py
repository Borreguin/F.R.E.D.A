# Created by Roberto Sanchez at 3/25/2019

import logging

logging.basicConfig(filename='app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def test():
    logging.warning("This an example of the use of this logger")


def logger():
    return logging


if __name__ == '__main__':
    test()
