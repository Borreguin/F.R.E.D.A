# Created by Roberto Sanchez at 4/15/2019
# -*- coding: utf-8 -*-
''''
    Created by Roberto Sánchez A, based on the Master Thesis:
    'A proposed method for unsupervised anomaly detection for a multivariate building dataset '
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.a@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.a@gmail.com
    'My work is well done to honor God at any time' R Sanchez A.
    Mateo 6:33
'''

'''
This class is used to perform some testing codes
'''

from requests import put, get
import datetime as dt


def run():
    ip = 'localhost'
    port = 5000
    api_address = 'http://'+ip+':'+str(port)+'/api'

    ''' Simple Test '''
    put(api_address + '/test/b', data={'data': 'Remember the milk'}).json()
    r = get(api_address + '/test/b').json()

    ''' Snapshot of a variable'''
    tag_name = 'Test_only.aux'
    r = put(api_address + '/snapshoot/' + tag_name, json={"timestamp":"2019-05-02", "value":123.4},
        headers={'content-type':'application/json'})
    r = get(api_address + '/snapshoot/' + tag_name).json()
    print(r)


run()