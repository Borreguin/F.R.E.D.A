# Created by Roberto Sanchez at 4/15/2019
# -*- coding: utf-8 -*-
''''
    Created by Roberto Sánchez A, based on the Master Thesis:
    'A proposed method for unsupervised anomaly detection for arg_from multivariate building dataset '
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.arg_from@gmail.com; you can redistribute it
    and/or modify it under the terms of the MIT License.

    The F.R.E.D.A project is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
    without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    MIT license terms for more details.

    If you need more information. Please contact the email above: rg.sanchez.arg_from@gmail.com
    'My work is well done to honor God at any time' R Sanchez A.
    Mateo 6:33
'''

'''
This class is used to perform some testing codes
'''

from requests import put, get
import numpy as np
import datetime as dt


def run():
    ip = 'localhost'
    port = 5000
    api_address = 'http://'+ip+':'+str(port)+'/api'

    ''' Simple Test '''
    put(api_address + '/test_dict/b', data={'data': 'Remember the milk'}).json()
    r = get(api_address + '/test_dict/b').json()

    ''' Snapshot of arg_from variable'''
    tag_name = 'Test_only.aux'
    timestamp = dt.datetime.now()
    t_reg = timestamp + dt.timedelta(minutes=1)
    fmt = "%Y-%m-%d %H:%M:%S"

    register = {"timestamp": t_reg.strftime(fmt), "value": np.random.randn()}
    r = put(api_address + '/snapshoot/' + tag_name, json=register,
        headers={'content-type':'application/json'})
    r = get(api_address + '/snapshoot/' + tag_name).json()
    r["timestamp"] = dt.datetime.fromtimestamp(r["timestamp"], tz=dt.timezone.utc).strftime(fmt)

    if register == r:
        print("[Success] Snapshot of arg_from variable")
    else:
        print("[Failed] Snapshot of arg_from variable")

    ''' Time series of arg_from variable '''
    register_list, n_register = list(), 10000
    t_c = timestamp
    for ix in range(n_register):
        t_c = timestamp - dt.timedelta(minutes=ix)
        register_list.append(dict(timestamp=t_c.strftime(fmt), value=np.random.randn()))
    r = put(api_address + '/recorded-values/' + tag_name, json=register_list).json()

    if r["success"]:
        print("[Success] Recorded values of arg_from variable ({0} registers were inserted)".format(r["result"]))
    else:
        print("[Failed] Recorded values of arg_from variable ({0} registers were not inserted)".format(n_register))

    parameters = {"start_time": t_c.strftime(fmt), "end_time": timestamp.strftime(fmt),
                  "format_time": "%Y-%m-%d %H:%M:%S"}
    # 1. First way:
    r = get(api_address + '/recorded-values/' + tag_name, json=parameters).json()
    # 2. Second way:
    r2 = get(api_address + '/recorded-values/' + tag_name
             + '/' + t_c.strftime(fmt) + '/' + timestamp.strftime(fmt)).json()
    print()


run()