""""
    Created by Roberto Sánchez A, based on his Master Thesis:
    "A proposed method for unsupervised anomaly detection for a multivariate building dataset "
    University of Bern/Neutchatel/Fribourg - 2017
    Any copy of this code should be notified at rg.sanchez.a@gmail.com
    to avoid intellectual property's problems.

    Not details about this code are included, if you need more information. Please contact the email above.
    "My work is well done to honor God at any time" R Sanchez A.
    Mateo 6:33
"""

import os
import ipyparallel as ipp
import time
import pandas as pd
import sys
sys.path.append('../')
from my_lib.hmm import hmm_util as hmm_u
import warnings

script_path = os.path.dirname(os.path.abspath(__file__))

DataPath = os.path.join(script_path, "data")
ModelPath = os.path.join(script_path, "model")
LogPath = os.path.join(script_path, "log")
config_file = os.path.join(script_path, "config.xlsx")
n_col_p = 24  # allow pivot if the number of columns is less than 'n_col_p'


import multiprocessing
from subprocess import call


def start_engines():
    # call("C:\inetpub\wwwroot\Gop_WebServer_production\hmm\start_ipcluster.bat")
    call(os.path.join(script_path, "start_ipcluster.bat"))


def stop_engines():
    # call("C:\inetpub\wwwroot\Gop_WebServer_production\hmm\stop_ipcluster.bat")
    call(os.path.join(script_path, "stop_ipcluster.bat"))


def d_time(time_reference):
    return time.time() - time_reference


def get_ipp_client(profile='default'):
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    rc = None
    try:
        rc = ipp.Client(profile=profile)
        print("Engines running for this client: {0}".format(rc.ids))
    except OSError or TimeoutError:
        print("Make sure the engines are running. "
              "To start manually the engines: \n $ipcluster start --profile=default -n 4")
    return rc


def main():
    tr = time.time()
    tc = time.time()
    rc = None
    dview = None
    df_config = None

    print("Start... ")
    print("DataPath: ", DataPath)
    print("ModelPath: ", ModelPath)
    print("LogPath: ", LogPath)
    print("ConfigPath: ", config_file)

    # start engines:
    stopipy = multiprocessing.Process(target=stop_engines)
    initipy = multiprocessing.Process(target=start_engines)
    initipy.start()

    # Try connection 2 times
    for t in range(2):
        try:
            # Sleep for 40 seconds before to get the client
            time.sleep(40)
            print('[{0:4.2f}] ---> Trying to connect to the engines:\n'.format(d_time(tr)))
            rc = get_ipp_client()
            dview = rc[:]
            print('[{0:4.2f}] Parallel client connected:\n'.format(d_time(tr)))
            break
        except Exception as e:
            print(e)

    if rc is None or len(rc) == 0:
        print("[{0:4.2f}] ---> Engines did not start correctly. Shutting down the engines. Try again, please. ".format(d_time(tr)))
        stopipy.start()
        return False

    data_set_file_names = None
    try:
        df_config = pd.read_excel(config_file)
        data_set_file_names = list(df_config["model_name"])
        data_set_file_names = [d.replace("hmm_1_", "") for d in data_set_file_names]
        df_config.index = data_set_file_names
    except Exception as e:
        print(e)

    if len(data_set_file_names) == 0:
        data_set_file_names = os.listdir(DataPath)
    print(data_set_file_names)
    print("")

    """ ________________________________________________________
        Setting engines:  
            Set CWD space to work
            Scatter and push variable to work with
    """

    v = rc.load_balanced_view()
    v.block = True
    v.map(os.chdir, [os.getcwd()] * len(v))
    """ os.getcwd(): Get the current CWD, ex: 'C:\\Repositorios\\parallel_programing' 
        os.chdir() : Set the current CWD """

    """ __________________________________________________________
        START: Defining the parallel function for training process 
    """

    @v.parallel(block=True)
    def hmm_model_training(idp):
        """
            :return the best model that was found in the training process using:
            idp, identify the interaction that is running (if is needed)
            @var_var_dataSet:       is a numpy array of shape (m x n) m features, n samples
                                    1 samples contains m features
            @var_var_list_nComp:    list of components to test and evaluate
            @var_index:             list of index that defines the evaluating dataset
            Note: To pass values to this function use: push and scatter methods.
        """
        import sys
        sys.path.append('../')
        import my_lib.hmm.hmm_util as hmm_u
        import numpy as np

        # Shared variables:
        global var_dataSet  # dview.push({'var_dataSet': dataset})
        global var_list_nComp  # dview.scatter('var_list_nComp', list(range(n_comp_min,n_comp_max+1)))
        global var_index  # dview.scatter('var_index', list(len(df.index)))
        global var_log_detail  # dview.push({'var_log_detail': fName})
        var_log_detail = var_log_detail.replace(".pkl", str(idp) + ".json")
        """ Taking a slide of the hole data set for validating purposes """
        validating_set = var_dataSet[var_index]
        """ Taking the rest of the dataSet for training purposes """
        ini, end = var_index[0], var_index[-1]
        training_set = np.concatenate((var_dataSet[0:ini], var_dataSet[end:-1]))
        best_model = None
        log_register = None
        try:
            """ Training a list of best models """
            best_model, log_register = hmm_u.select_best_HMM(training_set, validating_set, var_list_nComp, seed=idp)
            hmm_u.save_json_file(log_register, var_log_detail)
        except Exception as e:
            hmm_u.save_json_file(dict(error=str(e)), var_log_detail)
        """ Send the best model and a register/log of the training process """

        return {'model': best_model, "log_register": log_register}
        # return len(var_dataSet)

    """ ________________________________________________________
        END: Defining the parallel function for training process 
    """

    for fName in data_set_file_names:
        lst = ["1", "2"]
        for n in lst:
            tr = time.time()
            print(fName + ":")
            n_comp_min = df_config["n_comp_min_" + n].loc[fName]
            n_comp_max = df_config["n_comp_max_" + n].loc[fName]
            n_interaction = df_config["n_interaction_" + n].loc[fName]
            to_be_trained = df_config["Entrenamiento_" + n].loc[fName]

            if str(to_be_trained).upper() != "SI":
                continue

            fileName = n + "_" + fName

            """  Scattering the list of nComponents in current engines """
            dview.scatter('var_list_nComp', list(range(n_comp_min, n_comp_max + 1)))
            """  make sure that all process contains a scattered list of nComp """
            make_sure = dview.pull('var_list_nComp').get()  # important
            print("[{0:6f}] Going to train with nComponents in each engine: \n{1} \n".format(d_time(tr), make_sure))

            """ Reading and preparing dataset from DataPath"""
            try:
                df = pd.read_pickle(os.path.join(DataPath, fileName))
            except Exception as e:
                print("[{0:6f}]: {1} \n".format(d_time(tr), e))
                continue
            # allow pivot if the number of columns is less than 'n_col_p'
            if len(df.columns) < n_col_p:
                # transform dataset in format: [[sample1], [sample2],... , [sampleN]]
                df = hmm_u.pivot_DF_using_dates_and_hours(df)
            # df = df.head(100) #only 100 samples
            dataSet = df.values

            if len(dataSet) == 0:
                print("Check information for {0}".format(fileName))
                continue

            print("[{0:6.1f}] Training a HMM model for: \t\t{1}".format(d_time(tr), fileName))
            print("DataSet: " + str(len(dataSet)))
            tc = time.time()  # measuring training process
            """ Setting engines for the training process"""
            # push df.values in all engines to start training process:
            dview.push({'var_dataSet': dataSet})
            # for identifing the log:
            dview.push({'var_log_detail':  os.path.join(LogPath, fName)})
            # scattering indexes for validating purposes:
            dview.scatter('var_index', list(range(len(df.index))))
            dview.gather('var_index').get()  # make sure "var_index" is in engines.
            dview.pull('var_dataSet').get()

            """ Run the training process: (n_interaction) times in parallel fashion: """
            best_model_list = hmm_model_training(range(n_interaction * len(v) + 1))
            # print(best_model_list)
            final_model, log_register = hmm_u.select_best_model_from_list(best_model_list, dataSet)

            """ Ordering the best model according to a Hierarchical Clustering """
            ordered_model = hmm_u.ordered_hmm_model(final_model, method='average', metric='euclidean')

            """ Saving the best model and his log_register for posterior analysis """
            print("[{0:6.1f}] Select from final list ({2}) in: \t{1:4.2f}".format(d_time(tr), d_time(tc),
                                                                                  len(best_model_list)))
            hmm_u.save_model_and_log(ordered_model, log_register, ModelPath, LogPath, 'hmm_' + fileName)
            print("[{0:6.1f}] End training process in: \t\t{1:4.2f}\n ".format(d_time(tr), d_time(tc)))

    print('[{0:6.1f}] End of script '.format(d_time(tc)))
    stopipy.start()
    return True


if __name__ == "__main__":
    main()

# main()
