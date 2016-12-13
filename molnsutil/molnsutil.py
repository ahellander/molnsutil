"""
  Utility module for MOLNs.

  molnsutil contains implementations of a persisitent storage API for
  staging objects to an Object Store in the clouds supported by MOLNs.
  This can be used in MOLNs to write variables that are presistent
  between sessions, provides a convenetient way to get data out of the
  system, and it also provides a means during parallel computations to
  stage data so that it is visible to all compute engines, in contrast
  to using the local scratch space on the engines.

  molnsutil also contains parallel implementations of common Monte Carlo
  computational workflows, such as the generaion of ensembles and
  estimation of moments.

  Molnsutil will work for any object that is serializable (e.g. with
  pickle) and that has a run() function with the arguments 'seed' and
  'number_of_trajectories'.  Example:

   class MyClass():
       def run(seed, number_of_trajectories):
           # return an object or list

  Both the class and the results return from run() must be pickle-able.

"""


import boto
import boto.ec2
from os import environ
import logging
from boto.s3.connection import S3Connection
#logging.basicConfig(filename="boto.log", level=logging.DEBUG)
from boto.s3.key import Key
import uuid
import math
import molns_cloudpickle as cloudpickle
import random
import copy
import inspect

import swiftclient.client
import IPython.parallel
import uuid
from IPython.display import HTML, Javascript, display
import os
import sys

import itertools

class MolnsUtilException(Exception):
    pass

class MolnsUtilStorageException(Exception):
    pass

try:
    import dill as pickle
except:
    import pickle

import json


import multiprocessing
#     s3.json is a JSON file that contains the follwing info:
#
#     'aws_access_key_id' : AWS access key
#     'aws_secret_access_key' : AWS private key
#   s3.json needs to be created and put in .molns/s3.json in the root of the home directory.

import os
def get_persisistent_storage_config():
    """ Return the configuration for the persistent storage. """
    try:
        with open(os.environ['HOME']+'/.molns/s3.json','r') as fh:
            s3config = json.loads(fh.read())
        return s3config
    except IOError as e:
        logging.warning("Credentials file "+os.environ['HOME']+'/.molns/s3.json'+' missing. You will not be able to connect to S3 or Swift. Please create this file.')
        return {}



class LocalStorage():
    """ This class provides an abstraction for storing and reading objects on/from
        the ephemeral storage. """
    import molnsutil.molns_cloudpickle as cp


    def __init__(self, folder_name="/home/ubuntu/localarea"):
        self.folder_name = folder_name

    def put(self, filename, data):
        with open(self.folder_name+"/"+filename,'wb') as fh:
            cp.dump(data,fh)

    def get(self, filename):
        with open(self.folder_name+"/"+filename, 'rb') as fh:
            data = cp.load(fh)
        return data

    def delete(self,filename):
        os.remove(self.folder_name+"/"+filename)

class SharedStorage():
    """ This class provides an abstraction for storing and reading objects on/from
        the sshfs mounted storage on the controller. """
    import molnsutil.molns_cloudpickle as cp

    def __init__(self, serialization_method="cloudpickle"):
        self.folder_name = "/home/ubuntu/shared"
        self.serialization_method = serialization_method

    def put(self, filename, data):
        with open(self.folder_name+"/"+filename,'wb') as fh:
            if self.serialization_method == "cloudpickle":
                cp.dump(data,fh)
            elif self.serialization_method == "json":
                json.dump(data,fh)

    def get(self, filename):
        with open(self.folder_name+"/"+filename, 'rb') as fh:
            if self.serialization_method == "cloudpickle":
                data = cp.loads(fh.read())
            elif self.serialization_method == "json":
                data = json.loads(fh.read())
        return data

    def delete(self,filename):
        os.remove(self.folder_name+"/"+filename)


class S3Provider():
    def __init__(self, bucket_name):
        s3config = get_persisistent_storage_config()
        self.connection = S3Connection(is_secure=False,
                                 calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                                 **s3config['credentials']
                                 )
        self.set_bucket(bucket_name)

    def set_bucket(self, bucket_name=None):
        if bucket_name is None:
            self.bucket_name = "molns_bucket_{0}".format(str(uuid.uuid1()))
            bucket = self.connection.create_bucket(self.bucket_name)
        else:
            self.bucket_name = bucket_name
            bucket = self.connection.lookup(bucket_name)
            if bucket is None:
                bucket = self.connection.create_bucket(bucket_name)
            self.bucket = bucket

    def create_bucket(self,bucket_name):
        return self.connection.create_bucket(bucket_name)

    def put(self, name, data, reduced_redundancy=True):
        k = Key(self.bucket)
        if not k:
            raise MolnsUtilStorageException("Could not obtain key in the global store. ")
        k.key = name
        try:
            num_bytes = k.set_contents_from_string(data, reduced_redundancy=reduced_redundancy)
            if num_bytes == 0:
                raise MolnsUtilStorageException("No bytes written to key.")
        except Exception, e:
            return {'status':'failed', 'error':str(e)}
        return {'status':'success', 'num_bytes':num_bytes}

    def get(self, name, validate=False):
        k = Key(self.bucket,validate)
        k.key = name
        try:
            obj = k.get_contents_as_string()
        except boto.exception.S3ResponseError, e:
            raise MolnsUtilStorageException("Could not retrive object from the datastore."+str(e))
        return obj

    def delete(self, name):
        """ Delete an object. """
        k = Key(self.bucket)
        k.key = name
        self.bucket.delete_key(k)


    def delete_all(self):
        """ Delete all objects in the global storage area. """
        for k in self.bucket.list():
            self.bucket.delete_key(k.key)

    def list(self):
        """ List all containers. """
        return self.bucket.list()


class SwiftProvider():
    def __init__(self, bucket_name):
        s3config = get_persisistent_storage_config()
        self.connection = swiftclient.client.Connection(auth_version=2.0,**s3config['credentials'])
        self.set_bucket(bucket_name)

    def set_bucket(self,bucket_name):
        self.bucket_name = bucket_name
        if not bucket_name:
            self.bucket_name = "molns_bucket_{0}".format(str(uuid.uuid1()))
            bucket = self.provider.create_bucket(self.bucket_name)
        else:
            self.bucket_name = bucket_name
            try:
                bucket = self.connection.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.create_bucket(bucket_name)
                except Exception, e:
                    raise MolnsUtilStorageException("Failed to create/set bucket in the object store."+str(e))

            self.bucket = bucket


    def create_bucket(self, bucket_name):
        bucket = self.connection.put_container(bucket_name)
        return bucket

    def get_all_buckets(self):
        (response, bucket_list) = ps.provider.connection.get_account()
        return [b['name'] for b in bucket_list]

    def put(self, object_name, data):
        self.connection.put_object(self.bucket_name, object_name, data)

    def get(self, object_name, validate=False):
        (response, obj) = self.connection.get_object(self.bucket_name, object_name)
        return obj

    def delete(self, object_name):
        self.connection.delete_object(self.bucket_name, object_name)

    def delete_all(self):
        (response, obj_list) = self.connection.get_container(self.bucket_name)
        for obj in obj_list:
            self.connection.delete_object(self.bucket_name, obj['name'])
        return "{0} object deleted".format(len(obj_list))

    def list(self):
        (response, obj_list) = self.connection.get_container(self.bucket_name)
        return [obj['name'] for obj in obj_list]

    def close(self):
        self.connection.close()

    def __del__(self):
        self.close()


class PersistentStorage():
    """
       Provides an abstaction for interacting with the Object Stores
       of the supported clouds.
    """
    import molnsutil.molns_cloudpickle as cp


    def __init__(self, bucket_name=None):
        s3config = get_persisistent_storage_config()
        if bucket_name is None:
            # try reading it from the config file
            try:
                self.bucket_name = s3config['bucket_name']
            except:
                raise MolnsUtilStorageException("Could not find bucket_name in the persistent storage config.")
        else:
            self.bucket_name = bucket_name
        self.provider_type = s3config['provider_type']
        self.initialized = False

    def setup_provider(self):
        if self.initialized:
            return

        if self.provider_type == 'EC2':
            self.provider = S3Provider(self.bucket_name)
        # self.provider = S3Provider()
        elif self.provider_type == 'OpenStack':
            self.provider = SwiftProvider(self.bucket_name)
        else:
            raise MolnsUtilStorageException("Unknown provider type '{0}'.".format(self.provider_type))
        self.initialized = True

    def list_buckets(self):
        self.setup_provider()
        all_buckets=self.provider.get_all_buckets()
        buckets = []
        for bucket in all_buckets:
            buckets.append(bucket.name)
        return buckets

    def set_bucket(self,bucket_name=None):
        self.setup_provider()
        if not bucket_name:
            bucket = self.provider.create_bucket("molns_bucket_{0}".format(str(uuid.uuid1())))
        else:
            try:
                bucket = self.provider.get_bucket(bucket_name)
            except:
                try:
                    bucket = self.provider.create_bucket(bucket_name)
                except Exception, e:
                    raise MolnsUtilStorageException("Failed to create/set bucket in the object store: "+str(e))

        self.bucket = bucket

    def put(self, name, data):
        self.setup_provider()
        self.provider.put(name, cp.dumps(data))

    def get(self, name, validate=False):
        self.setup_provider()
        return cp.loads(self.provider.get(name, validate))

    def delete(self, name):
        """ Delete an object. """
        self.setup_provider()
        self.provider.delete(name)

    def list(self):
        """ List all containers. """
        self.setup_provider()
        return self.provider.list()

    def delete_all(self):
        """ Delete all objects in the global storage area. """
        self.setup_provider()
        self.provider.delete_all()


class CachedPersistentStorage(PersistentStorage):
    import molnsutil.molns_cloudpickle as cp
    
    def __init__(self, bucket_name=None):
        PersistentStorage.__init__(self,bucket_name)
        self.cache = LocalStorage(folder_name = "/mnt/molnsarea/cache")

    def get(self, name, validate=False):
        self.setup_provider()
        # Try to read it form cache
        try:
            data = cp.loads(self.cache.get(name))
        except: # if not there, read it from the Object Store and write it to the cache
            data = cp.loads(self.provider.get(name, validate))
            try:
                self.cache.put(name, data)
            except:
                # For now, we just ignore errors here, like if the disk is full...
                pass
        return data

# TODO: Extend the delete methods so that they also delete the file from cache
# TODO: Implement clear_cache(self) - delete all files from Local Cache.

#------  default aggregators -----
def builtin_aggregator_list_append(new_result, aggregated_results=None, parameters=None):
    """ default chunk aggregator. """
    if aggregated_results is None:
        aggregated_results = []
    aggregated_results.append(new_result)
    return aggregated_results

def builtin_aggregator_add(new_result, aggregated_results=None, parameters=None):
    """ chunk aggregator for the mean function. """
    if aggregated_results is None:
        return (copy.deepcopy(new_result), 1)
    return (aggregated_results[0]+new_result, aggregated_results[1]+1)

def builtin_aggregator_sum_and_sum2(new_result, aggregated_results=None, parameters=None):
    """ chunk aggregator for the mean+variance function. """
    if aggregated_results is None:
        return (new_result, new_result**2, 1)
    return (aggregated_results[0]+new_result, aggregated_results[1]+new_result**2, aggregated_results[2]+1)

def builtin_reducer_default(result_list, parameters=None):
    """ Default passthrough reducer. """
    return result_list

def builtin_reducer_mean(result_list, parameters=None):
    """ Reducer to calculate the mean, use with 'builtin_aggregator_add' aggregator. """
    sum = 0.0
    n = 0.0
    for r in result_list:
        sum += r[0]
        n += r[1]
    return sum/n

def builtin_reducer_mean_variance(result_list, parameters=None):
    """ Reducer to calculate the mean and variance, use with 'builtin_aggregator_sum_and_sum2' aggregator. """
    sum = 0.0
    sum2 = 0.0
    n = 0.0
    for r in result_list:
        sum += r[0]
        sum2 += r[1]
        n += r[2]
    return (sum/n, (sum2 - (sum**2)/n)/n )


#----- functions to use for the DistributedEnsemble class ----
def run_ensemble_map_and_aggregate(model_class, parameters, param_set_id, seed_base, number_of_trajectories, mapper, aggregator=None):
    """ Generate an ensemble, then run the mappers are aggreator.  This will not store the results. """
    import sys
    import uuid
    import molnsutil.molns_cloudpickle as cp

    if aggregator is None:
        aggregator = builtin_aggregator_list_append
    # Create the model
    try:
        model_class_cls = cp.loads(model_class)
        if parameters is not None:
            model = model_class_cls(**parameters)
        else:
            model = model_class_cls()
    except Exception as e:
        notes = "Error instantiation the model class, caught {0}: {1}\n".format(type(e),e)
        notes +=  "dir={0}\n".format(dir())
        raise MolnsUtilException(notes)
    # Run the solver
    res = None
    num_processed = 0
    results = model.run(seed=seed_base, number_of_trajectories=number_of_trajectories)
    if not isinstance(results, list):
        results = [results]
    #for i in range(number_of_trajectories):
    for result in results:
        try:
            mapres = mapper(result)
            res = aggregator(mapres, res)
            num_processed +=1
        except Exception as e:
            notes = "Error running mapper and aggregator, caught {0}: {1}\n".format(type(e),e)
            notes += "type(mapper) = {0}\n".format(type(mapper))
            notes += "type(aggregator) = {0}\n".format(type(aggregator))
            notes +=  "dir={0}\n".format(dir())
            raise MolnsUtilException(notes)
    return {'result':res, 'param_set_id':param_set_id, 'num_sucessful':num_processed, 'num_failed':number_of_trajectories-num_processed}


def write_file(storage_mode,filename, result):
    
    from molnsutil import LocalStorage, SharedStorage, PersistentStorage

    if storage_mode=="Shared":
        storage  = SharedStorage()
    elif storage_mode=="Persistent":
        storage = PersistentStorage()
    else:
        raise MolnsUtilException("Unknown storage type '{0}'".format(storage_mode))
    
    storage.put(filename, result)

def run_ensemble(model_class, parameters, param_set_id, seed_base, number_of_trajectories, storage_mode="Shared"):
    """ Generates an ensemble consisting of number_of_trajectories realizations by
        running the model 'nt' number of times. The resulting result objects
        are serialized and written to one of the MOLNs storage locations, each
        assigned a random filename. The default behavior is to write the
        files to the Shared storage location (global non-persistent). Optionally, files can be
        written to the Object Store (global persistent), storage_model="Persistent"

        Returns: a list of filenames for the serialized result objects.

        """

    import sys
    import uuid
    from molnsutil import PersistentStorage, LocalStorage, SharedStorage
    import molnsutil.molns_cloudpickle as cp


    if storage_mode=="Shared":
        storage  = SharedStorage()
    elif storage_mode=="Persistent":
        storage = PersistentStorage()
    else:
        raise MolnsUtilException("Unknown storage type '{0}'".format(storage_mode))
    # Create the model
    try:
        model_class_cls = cp.loads(model_class)
        if parameters is not None:
            model = model_class_cls(**parameters)
        else:
            model = model_class_cls()
    except Exception as e:
        notes = "Error instantiation the model class, caught {0}: {1}\n".format(type(e),e)
        notes +=  "dir={0}\n".format(dir())
        raise MolnsUtilException(notes)

    # Run the solver
    filenames = []
    processes=[]
    results = model.run(seed=seed_base, number_of_trajectories=number_of_trajectories)
    if not isinstance(results, list):
        results = [results]
    for result in results:
        try:
            # We should try to thread this to hide latency in file upload...
            filename = str(uuid.uuid1())
            storage.put(filename, result)
            filenames.append(filename)
        except:
            raise

    return {'filenames':filenames, 'param_set_id':param_set_id}

def map_and_aggregate(results, param_set_id, mapper, aggregator=None, cache_results=False):
    """ Reduces a list of results by applying the map function 'mapper'.
        When this function is applied on an engine, it will first
        look for the result object in the local ephemeral storage (cache),
        then in the Shared area (global non-persistent), then in the
        Object Store (global persistent).

        If cache_results=True, then result objects will be written
        to the local epehemeral storage (file cache), so subsequent
        postprocessing jobs may run faster.

        """
    import dill
    import numpy
    from molnsutil import PersistentStorage, LocalStorage, SharedStorage
    ps = PersistentStorage()
    ss = SharedStorage()
    ls = LocalStorage()
    if aggregator is None:
        aggregator = builtin_aggregator_list_append
    num_processed=0
    res = None
    result = None

    for i,filename in enumerate(results):
        enotes = ''
        result = None
        try:
            result = ls.get(filename)
        except Exception as e:
            enotes += "In fetching from local store, caught  {0}: {1}\n".format(type(e),e)

        if result is None:
            try:
                result = ss.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except Exception as e:
                enotes += "In fetching from shared store, caught  {0}: {1}\n".format(type(e),e)
        if result is None:
            try:
                result = ps.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except Exception as e:
                enotes += "In fetching from global store, caught  {0}: {1}\n".format(type(e),e)
        if result is None:
            notes = "Error could not find file '{0}' in storage\n".format(filename)
            notes += enotes
            raise MolnsUtilException(notes)

        try:
            mapres = mapper(result)
            res = aggregator(mapres, res)
            num_processed +=1
        except Exception as e:
            notes = "Error running mapper and aggregator, caught {0}: {1}\n".format(type(e),e)
            notes += "type(mapper) = {0}\n".format(type(mapper))
            notes += "type(aggregator) = {0}\n".format(type(aggregator))
            notes +=  "dir={0}\n".format(dir())
            raise MolnsUtilException(notes)

    return {'result':res, 'param_set_id':param_set_id, 'num_sucessful':num_processed, 'num_failed':len(results)-num_processed}

    #return res

############################################################################
class DistributedEnsemble():
    """ A class to provide an API for execution of a distributed ensemble. """

    my_class_name = 'DistributedEnsemble'

    #-----------------------------------------------------------------------------------
    @classmethod
    def delete(cls, name):
        """ Static method to remove the state of a distributed comptutation from the system."""
        # delete realization
        try:
            with open('.molnsutil/{1}-{0}'.format(name, cls.my_class_name)) as fd:
                state = pickle.load(fd)
                
                if state['storage_mode'] is not None:
                    if state['storage_mode'] == "Shared":
                        ss = SharedStorage()
                    elif state['storage_mode'] == "Persistent":
                        ss = PersistentStorage()
                    for param_set_id in state['result_list']:
                        for filename in state['result_list'][param_set_id]:
                            try:
                                ss.delete(filename)
                            except OSError as e:
                                pass
            # delete database file
            os.remove('.molnsutil/{1}-{0}'.format(name, cls.my_class_name))
        except Exception as e:
            sys.stderr.write('delete(): {0}'.format(e))

    #-----------------------------------------------------------------------------------
    def __init__(self, name=None, model_class=None, parameters=None, client=None, num_engines=None, ignore_model_mismatch=False):
        """ Constructor """
        if not isinstance(name, str):
            raise MolnsUtilException("name not specified")
        self.name = name
        if not inspect.isclass(model_class):
            raise MolnsUtilException("model_class not a class")
        self.model_class = cloudpickle.dumps(model_class)
        # Set the Ipython.parallel client
        self.num_engines = num_engines
        self._update_client(client)
        
        if not self.load_state(ignore_model_mismatch=ignore_model_mismatch):
            # Set defaults if state not found
            self.parameters = [parameters]
            self.number_of_trajectories = 0
            self.seed_base = self.generate_seed_base()
            self.storage_mode = None
            self.result_list = {}
            self.running_MapReduceTask = None
            self.running_SimulationTask = None
            self.reduced_results = None
            self.mapped_results = None
            self.number_of_results = 0
            self.step1_complete = False
            self.step2_complete = False
            self.step3_complete = False
            self.mapper_fn = None
            self.aggregator_fn  = None
            self.reducer_fn = None

    #-----------------------------------------------------------------------------------
    def generate_seed_base(self):
        """ Create a random number and truncate to 64 bits. """
        return abs(int(random.getrandbits(31)))

    #-----------------------------------------------------------------------------------
    def save_state(self):
        """ Serialize the state of the ensemble, for persistence beyond memory."""
        state = {}
        state['model_class'] = self.model_class
        state['parameters'] = self.parameters
        state['number_of_trajectories'] = self.number_of_trajectories
        state['seed_base'] = self.seed_base
        state['result_list'] = self.result_list
        state['storage_mode'] = self.storage_mode
        #
        state['reduced_results'] = self.reduced_results
        state['mapped_results'] = self.mapped_results
        state['number_of_results'] = self.number_of_results
        state['step1_complete'] = self.step1_complete
        state['step2_complete'] = self.step2_complete
        state['step3_complete'] = self.step3_complete
        state['mapper_fn'] = self.mapper_fn
        state['aggregator_fn'] = self.aggregator_fn
        state['reducer_fn'] = self.reducer_fn
        #
        if self.running_MapReduceTask is None:
            state['running_MapReduceTask'] = None
        else:
            state['running_MapReduceTask'] = self.running_MapReduceTask.msg_ids
        if self.running_SimulationTask is None:
            state['running_SimulationTask'] = None
        else:
            state['running_SimulationTask'] = self.running_SimulationTask.msg_ids
        if not os.path.isdir('.molnsutil'):
            os.makedirs('.molnsutil')
        with open('.molnsutil/{1}-{0}'.format(self.name, self.my_class_name),'w+') as fd:
            pickle.dump(state, fd)

    #-----------------------------------------------------------------------------------
    def load_state(self, ignore_model_mismatch=False):
        """ Recover the state of an ensemble from a previous save. """
        try:
            with open('.molnsutil/{1}-{0}'.format(self.name, self.my_class_name)) as fd:
                state = pickle.load(fd)
            if state['model_class'] != self.model_class and not ignore_model_mismatch:
                #sys.stderr.write("Error loading saved state\n\n")
                #sys.stderr.write("state['model_class']={0}\n\n".format(state['model_class']))
                #sys.stderr.write("self.model_class={0}\n\n".format(self.model_class))
                #sys.stderr.write("state['model_class'] != self.model_class {0}\n\n".format(state['model_class'] != self.model_class))
                #TODO: Minor differences show up in the pickled string, but the classes are still identical.  Find a way around this.
                raise MolnsUtilException("Can only load state of a class that is identical to the original class. Use '{0}.delete(name=\"{1}\")' to remove previous state.  Use the argument 'ignore_model_mismatch=True' to override".format(self.my_class_name, self.name))
                #TODO: Check to be sure the state is sane.  Both tasks can not be running, result list and number_of_trajectories should match up, (others?).
            
            self.parameters = state['parameters']
            self.number_of_trajectories = state['number_of_trajectories']
            self.seed_base = state['seed_base']
            self.result_list = state['result_list']
            self.storage_mode = state['storage_mode']
            #
            self.reduced_results = state['reduced_results']
            self.mapped_results = state['mapped_results']
            self.number_of_results = state['number_of_results']
            self.step1_complete = state['step1_complete']
            self.step2_complete = state['step2_complete']
            self.step3_complete = state['step3_complete']
            self.mapper_fn = state['mapper_fn']
            self.aggregator_fn  = state['aggregator_fn']
            self.reducer_fn = state['reducer_fn']
            #
            if state['running_MapReduceTask'] is None:
                self.running_MapReduceTask = None
            else:
                self.running_MapReduceTask = self.c.get_result(state['running_MapReduceTask'])
            if state['running_SimulationTask'] is None:
                self.running_SimulationTask = None
            else:
                self.running_SimulationTask = self.c.get_result(state['running_SimulationTask'])
            return True
        except IOError as e:
            return False

    #-----------------------------------------------------------------------------------
    # MAIN FUNCTION
    #-----------------------------------------------------------------------------------
    def run(self, mapper=None, aggregator=None, reducer=None, number_of_trajectories=None, chunk_size=None, verbose=True, progress_bar=True, store_realizations=True, storage_mode="Shared", cache_results=False):
        """ Main entry point for executing parallel MOLNs computations.
        
        Arguments:
            mapper                      [required] Python function that takes as input the simulation result. This function is applied to each simulation trajectory.
            aggregator                  [optional] Python function that aggregates the output of the mapper function on each worker engine.
            reducer                     [optional] Python function that takes as input the output of all the aggregator functions on each worker.  One insteance of this function is run for each parameter point.
            number_of_trajectories      [required] Integer, number of simulation trajectories to execute for each parameter point.
            chunk_size                  [optional] Integer, group a number of trajectories into a block for efficicnet execution. One aggregator will be run for each chunk.
            verbose                     [optional, default True] Print the status of the computation.
            progress_bar                [optional, default True] Display a javascript progress bar to indicate the progress of the computation.
            store_realizations          [optional, default True] If set to False, the intermediary results will be deleted as soon as the computation is complete.
            storage_mode                [required, default 'Shared'] Either 'Shared' or 'Persistent'. Store the intermediary results in the ephemeral shared filesystem ('Shared'), or the cloud object store ('Persistent' e.g. Amazon S3 or OpenStack Swift).
            cache_results               [optional, experimental, default False] Store the intermediary results in the ephemeral storage on each compute node.
        Returns:
            The output of the computation is returned.  For 'DistributedEnsemble' class, this is the output of the 'reducer' function.  For the 'ParameterSweep' class, this is a 'ParameterSweepResultList' object.
        Raises:
            MolnsUtilException on error.
        """
        #####
        # 0. Validate input.
        if mapper is None or not hasattr(mapper, '__call__'):
            raise MolnsUtilException("mapper function not specified")
        if self.storage_mode is None:
            if storage_mode != "Persistent" and storage_mode != "Shared":
                raise MolnsUtilException("Acceptable values for 'storage_mode' are 'Persistent' or 'Shared'")
            self.storage_mode = storage_mode
        elif self.storage_mode != storage_mode:
            raise MolnsUtilException("Storage mode already set to {0}, can not mix storage modes".format(self.storage_mode))
        if number_of_trajectories is None or number_of_trajectories == 0:
            raise MolnsUtilException("'number_of_trajectories' is zero.")
        elif not store_realizations and self.number_of_trajectories > 0 and self.number_of_trajectories != number_of_trajectories:
            raise MolnsUtilException("'number_of_trajectories' changed since first call.  Value can only be changed if store_realizations is True")
            #TODO: Fix, if store_realizations is false, number_of_trajectories=0 after success
        elif store_realizations and self.number_of_trajectories > 0 and self.number_of_trajectories > number_of_trajectories:
            raise MolnsUtilException("'number_of_trajectories' less than first call. Can not reduce number of realizations.")
        # Check if the mapper, aggregator, and reducer functions are the same as previously run.  If not throw error.  Use 'clear_results()' to reset
        mapper_fn_pkl = cloudpickle.dumps(mapper)
        if self.mapper_fn is not None and self.mapper_fn != mapper_fn_pkl:
            raise MolnsUtilException("'mapper' function has changed since results have been computed.  Use 'clear_results()' to reset.")
        else:
            self.mapper_fn = mapper_fn_pkl
        aggregator_fn_pkl = cloudpickle.dumps(aggregator)
        if self.aggregator_fn is not None and self.aggregator_fn != aggregator_fn_pkl:
            raise MolnsUtilException("'aggregator' function has changed since results have been computed.  Use 'clear_results()' to reset.")
        else:
            self.aggregator_fn = aggregator_fn_pkl
        reducer_fn_pkl = cloudpickle.dumps(reducer)
        if self.reducer_fn is not None and self.reducer_fn != reducer_fn_pkl:
            raise MolnsUtilException("'reducer' function has changed since results have been computed.  Use 'clear_results()' to reset.")
        else:
            self.reducer_fn = reducer_fn_pkl
        if self.number_of_trajectories > 0 and not store_realizations and self.number_of_trajectories != number_of_trajectories:
            raise MolnsUtilException("'number_of_trajectories' is not the same as the original call. It can only be changed if 'store_realizations' is True.")

        #####
        # Shortcut, check if computation is complete
        if self.step3_complete:
            if verbose:
                print "Using previously computed results."
            return self.reduced_results


        ######
        # 1. Run simulations
        #sys.stderr.write("[1] self.number_of_trajectories < number_of_trajectories: {0} {1} {2}\n".format(self.number_of_trajectories,number_of_trajectories,self.number_of_trajectories < number_of_trajectories))
        #sys.stderr.write("[1] (not self.step1_complete): {0} {1}\n".format(self.step1_complete, (not store_realizations)))
        if (not self.step1_complete) or (store_realizations == True and self.number_of_trajectories < number_of_trajectories):
            if verbose:
                print "Step 1: Computing simulation trajectories."
            self.add_realizations( number_of_trajectories - self.number_of_trajectories, chunk_size=chunk_size, verbose=verbose, storage_mode=storage_mode, progress_bar=progress_bar)

            self.step1_complete=True
        else:
            if verbose:
                print "Step 1: Done. Using previously computed simulation trajectories."

        ######
        # 2. Run Map function for the MapReduce post-processing
        if (not self.step2_complete) or (store_realizations == True and self.number_of_results < self.number_of_trajectories):
            if verbose:
                print "Step 2: Running mapper & aggregator on the result objects (number of results={0}, chunk size={1})".format(self.number_of_trajectories*len(self.parameters), chunk_size)
            self.run_mappers(mapper, aggregator, chunk_size=chunk_size, progress_bar=progress_bar, cache_results=cache_results)
            self.step2_complete=True
            self.step3_complete=False # To ensure that step 3 is always run after step 2 runs (as in the case of a re-execution).
        else:
            if verbose:
                print "Step 2: Done. Using previously computed results."

        ######
        # 3. Run Reduce function for the MapReduce post-processing
        if not self.step3_complete:
            if verbose:
                print "Step 3: Running reducer on mapped and aggregated results (size={0})".format(len(self.mapped_results[0]))
            if reducer is None:
                reducer = builtin_reducer_default
            self.reduced_results = self.run_reducer(reducer)
            self.step3_complete = True
            self.save_state()
        else:
            if verbose:
                print "Step 3: Done. Using previously computed results."

        ######
        # Clean up
        if not store_realizations:
            self.delete_realizations()
            self.delete_results()
            self.save_state()

        ######
        # Return results
        return self.reduced_results

    #-----------------------------------------------------------------------------------
    def run_reducer(self, reducer):
        """ Inside the run() function, apply the reducer to all of the map'ped-aggregated result values. """
        return reducer(self.mapped_results[0], parameters=self.parameters[0])

    #-----------------------------------------------------------------------------------
    def run_mappers(self, mapper, aggregator, chunk_size=None, progress_bar=True, cache_results=False):
        """ Run the mapper and aggrregator function on each of the simulation trajectories. """
        num_results_to_compute = self.number_of_trajectories - self.number_of_results
        if self.running_MapReduceTask is None:
            #sys.stderr.write('run_mappers(): Starting MapReduce Task\n')
            # If number_of_trajectories > 0 and number_of_results < number_of_trajectories, only run mappers on the 'new' trajectories.
            offset = self.number_of_results
            # chunks per parameter
            if chunk_size is None:
                chunk_size = self._determine_chunk_size(num_results_to_compute)
            #sys.stderr.write('chunk_size={0}, num_results_to_compute={1}\n'.format(chunk_size, num_results_to_compute))
            if chunk_size < 1: chunk_size = 1
            num_chunks = int(math.ceil(num_results_to_compute/float(chunk_size)))
            chunks = [chunk_size]*(num_chunks-1)
            chunks.append(num_results_to_compute-chunk_size*(num_chunks-1))
            # total chunks
            pchunks = chunks*len(self.parameters)
            num_pchunks = num_chunks*len(self.parameters)
            #pparams = []
            param_set_ids = []
            presult_list = []
            try:  #'try' is for Debugging
                for id, param in enumerate(self.parameters):
                    param_set_ids.extend( [id]*num_chunks )
                    #pparams.extend( [param]*num_chunks )
                    for i in range(num_chunks):
                        presult_list.append( self.result_list[id][(i*chunk_size+offset):((i+1)*chunk_size+offset)] )
            except Exception as e:
                #sys.stderr.write('run_mappers(): caught exception while trying start MapReduce Task: {0}\n'.format(e))
                #sys.stderr.write('run_mappers(): self.parameters={0}\n'.format(self.parameters))
                #sys.stderr.write('run_mappers(): self.result_list={0}\n'.format(self.result_list))
                raise

            self.running_MapReduceTask = self.lv.map_async(map_and_aggregate, presult_list, param_set_ids, [mapper]*num_pchunks,[aggregator]*num_pchunks,[cache_results]*num_pchunks)
            self.save_state()
        else:
            #sys.stderr.write('run_mappers(): MapReduce Task already running\n')
            pass
        


        if progress_bar:
            divid = str(uuid.uuid4())
            pb = HTML("""
                          <div style="border: 1px solid black; width:500px">
                          <div id="{0}" style="background-color:blue; width:0%">&nbsp;</div>
                          </div>
                          """.format(divid))
            display(pb)
            
            #sys.stderr.write('run_mappers(): running_MapReduceTask.ready()={0}\n'.format(self.running_MapReduceTask.ready()))
            while not self.running_MapReduceTask.ready():
                self.running_MapReduceTask.wait(timeout=1)
                progress = 100.0 * self.running_MapReduceTask.progress / len(self.running_MapReduceTask)
                display(Javascript("$('div#%s').width('%f%%')" % (divid, 100.0*(+1)/len(self.running_MapReduceTask))))
            display(Javascript("$('div#%s').width('%f%%')" % (divid, 100.0)))
            #sys.stderr.write('run_mappers(): running_MapReduceTask.ready()={0}\n'.format(self.running_MapReduceTask.ready()))
        else:
            #sys.stderr.write('run_mappers(): waiting for MapReduce Task to complete (no progress bar)\n')
            self.running_MapReduceTask.wait()

    
        #sys.stderr.write("\n\nself.running_MapReduceTask.result={0}\n\n".format(self.running_MapReduceTask.result))
        # Process the results.
        cnt=0
        self.mapped_results = {}
        for i,rset in enumerate(self.running_MapReduceTask.result):
            
            param_set_id = rset['param_set_id']
            r = rset['result']
            if param_set_id not in self.mapped_results:
                self.mapped_results[param_set_id] = []
            if type(r) is type([]):
                self.mapped_results[param_set_id].extend(r) #if a list is returned, extend that list
            else:
                self.mapped_results[param_set_id].append(r)
            cnt+=len(r)
            
        if cnt != num_results_to_compute*len(self.parameters):
            raise MolnsUtilException('run_mappers() num_results_to_compute={0} len(parameters)={1}, got {2} results'.format(num_results_to_compute, len(self.parameters), cnt))
        
        self.number_of_results = self.number_of_trajectories
        # Set the state to not running
        self.running_MapReduceTask = None
        self.save_state()

    #-----------------------------------------------------------------------------------
    def add_realizations(self, number_of_trajectories=None, chunk_size=None, verbose=True, progress_bar=True, storage_mode="Shared"):
        """ Add a number of realizations to the ensemble. """
        if number_of_trajectories is None:
            raise MolnsUtilException("No number_of_trajectories specified")
        if type(number_of_trajectories) is not type(1):
            raise MolnsUtilException("number_of_trajectories must be an integer")

        if chunk_size is None:
            chunk_size = self._determine_chunk_size(number_of_trajectories)

        if verbose:
            if len(self.parameters) > 1:
                print "Generating {0} realizations of the model at {1} parameter points (chunk size={2})".format(number_of_trajectories, len(self.parameters), chunk_size)
            else:
                print "Generating {0} realizations of the model (chunk size={1})".format(number_of_trajectories,chunk_size)
            
        if self.running_SimulationTask is None:
            #sys.stderr.write('add_realizations(): Starting Simulation Task\n')
            num_chunks = int(math.ceil(number_of_trajectories/float(chunk_size)))
            chunks = [chunk_size]*(num_chunks-1)
            chunks.append(number_of_trajectories-chunk_size*(num_chunks-1))
            # total chunks
            pchunks = chunks*len(self.parameters)
            num_pchunks = num_chunks*len(self.parameters)
            pparams = []
            param_set_ids = []
            for id, param in enumerate(self.parameters):
                param_set_ids.extend( [id]*num_chunks )
                pparams.extend( [param]*num_chunks )

            seed_list = []
            for _ in range(len(self.parameters)):
                #need to do it this way cause the number of run per chunk might not be even
                seed_list.extend(range(self.seed_base, self.seed_base+number_of_trajectories, chunk_size))
                self.seed_base += number_of_trajectories
            self.running_SimulationTask  = self.lv.map_async(run_ensemble, [self.model_class]*num_pchunks, pparams, param_set_ids, seed_list, pchunks, [storage_mode]*num_pchunks)
            self.save_state()
        else:
            #sys.stderr.write('add_realizations(): Simulation Task already running\n')
            pass

        if progress_bar:
            divid = str(uuid.uuid4())
            pb = HTML("""
                          <div style="border: 1px solid black; width:500px">
                          <div id="{0}" style="background-color:blue; width:0%">&nbsp;</div>
                          </div>
                          """.format(divid))
            display(pb)
            
            #sys.stderr.write("add_realizations(): running_SimulationTask.ready()={0}\n".format(self.running_SimulationTask.ready()))
            while not self.running_SimulationTask.ready():
                self.running_SimulationTask.wait(timeout=1)
                progress = 100.0 * self.running_SimulationTask.progress / len(self.running_SimulationTask)
                display(Javascript("$('div#%s').width('%f%%')" % (divid, 100.0*(+1)/len(self.running_SimulationTask))))
            display(Javascript("$('div#%s').width('%f%%')" % (divid, 100.0)))
            #sys.stderr.write("add_realizations(): running_SimulationTask.ready()={0}\n".format(self.running_SimulationTask.ready()))
        else:
            #sys.stderr.write('add_realizations(): waiting for Simulation Task to complete (no progress bar)\n')
            self.running_SimulationTask.wait()


        #sys.stderr.write("\n\nself.running_SimulationTask.result={0}\n\n".format(self.running_SimulationTask.result))
        # We process the results as they arrive.
        cnt=0
        for i,ret in enumerate(self.running_SimulationTask.result):
            r = ret['filenames']
            param_set_id = ret['param_set_id']
            if param_set_id not in self.result_list:
                self.result_list[param_set_id] = []
            self.result_list[param_set_id].extend(r)
            cnt+=len(r)
        #sys.stderr.write('add_realizations(): stored {0} simulation results\n'.format(cnt))
        if cnt != number_of_trajectories*len(self.parameters):
            raise MolnsUtilException('add_realizations() number_of_trajectories={0} len(parameters)={1}, got {2} results'.format(number_of_trajectories, len(self.parameters), cnt))
            
        wall_time = self.running_SimulationTask.wall_time
        serial_time = self.running_SimulationTask.serial_time
        self.number_of_trajectories += number_of_trajectories
        self.running_SimulationTask = None
        self.save_state()

        #sys.stderr.write('add_realizations(): self.result_list has {0} keys\n'.format(len(self.result_list)))


        return {'wall_time':wall_time,'serial_time':serial_time}



    #-----------------------------------------------------------------------------------
    #-------- Convenience functions with builtin mappers/reducers  ------------------
    #-----------------------------------------------------------------------------------

    def mean_variance(self, mapper=None, number_of_trajectories=None, chunk_size=None, verbose=True, store_realizations=True, storage_mode="Shared", cache_results=False):
        """ Compute the mean and variance (second order central moment) of the function g(X) based on number_of_trajectories realizations
            in the ensemble. """
        return self.run(mapper=mapper, aggregator=builtin_aggregator_sum_and_sum2, reducer=builtin_reducer_mean_variance, number_of_trajectories=number_of_trajectories, chunk_size=chunk_size, verbose=verbose, store_realizations=store_realizations, storage_mode=storage_mode, cache_results=cache_results)

    def mean(self, mapper=None, number_of_trajectories=None, chunk_size=None, verbose=True, store_realizations=True, storage_mode="Shared", cache_results=False):
        """ Compute the mean of the function g(X) based on number_of_trajectories realizations
            in the ensemble. It has to make sense to say g(result1)+g(result2). """
        return self.run(mapper=mapper, aggregator=builtin_aggregator_add, reducer=builtin_reducer_mean, number_of_trajectories=number_of_trajectories, chunk_size=chunk_size, verbose=verbose, store_realizations=store_realizations, storage_mode=storage_mode, cache_results=cache_results)


    def moment(self, g=None, order=1, number_of_trajectories=None):
        """ Compute the moment of order 'order' of g(X), using number_of_trajectories
            realizations in the ensemble. """
        raise Exception('TODO')

    def histogram_density(self, g=None, number_of_trajectories=None):
        """ Estimate the probability density function of g(X) based on number_of_trajectories realizations
            in the ensemble. """
        raise Exception('TODO')

    #-----------------------------------------------------------------------------------
    #-----------------------------------------------------------------------------------

    def _update_client(self, client=None):
        """ Setup the IPython.parallel.Client() object. """
        if client is None:
            self.c = IPython.parallel.Client(profile='default')
        else:
            self.c = client
        if len(self.c.ids) == 0:
            raise MolnsUtilException("No parallel engines detected.  Molnsutil will not work in this situation.  Do you need to start more MOLNs workers?")
        self.c[:].use_dill()
        if self.num_engines == None:
            self.lv = self.c.load_balanced_view()
            self.num_engines = len(self.c.ids)
        else:
            max_num_engines = len(self.c.ids)
            if self.num_engines > max_num_engines:
                self.num_engines = max_num_engines
                self.lv = self.c.load_balanced_view()
            else:
                engines = self.c.ids[:self.num_engines]
                self.lv = self.c.load_balanced_view(engines)

        # Start a spin thread
        self.c.stop_spin_thread()
        self.c.spin_thread(interval=10)
        # Set the number of times a failed task is retried. This makes it possible to recover
        # from engine failure.
        self.lv.retries=3

    def _determine_chunk_size(self, number_of_trajectories):
        """ Determine a optimal chunk size. """
        return min(int(max(1, round(number_of_trajectories/float(self.num_engines)))),1)

    def _clear_cache(self):
        """ Remove all cached result objects on the engines. """
        pass
        # TODO

    def clear_results(self):
        """ Remove the output from the Mappers and Reducers from the storge."""
        self.delete_results()
        self.mapper_fn = None
        self.aggregator_fn = None
        self.reducer_rn = None
        self.step3_complete = False
        self.save_state()

    def delete_results(self):
        """ Delete final results of the computation. """
        self.step2_complete = False
        self.mapped_results = {}
        self.number_of_results = 0

    def delete_realizations(self):
        """ Delete realizations from the storage. """
        if self.storage_mode is None:
            return
        elif self.storage_mode == "Shared":
            ss = SharedStorage()
        elif self.storage_mode == "Persistent":
            ss = PersistentStorage()

        for param_set_id in self.result_list:
            for filename in self.result_list[param_set_id]:
                try:
                    ss.delete(filename)
                except OSError as e:
                    pass
        self.step1_complete = False
        self.result_list = {}
        self.number_of_trajectories = 0

    def __del__(self):
        """ Deconstructor. """
        pass

############################################################################
class ParameterSweep(DistributedEnsemble):
    """ Making parameter sweeps on distributed compute systems easier. """
    
    my_class_name = 'ParameterSweep'

    def __init__(self, name=None, model_class=None, parameters=None, client=None, num_engines=None, ignore_model_mismatch=False):
        """ Constructor.
        Args:
          model_class: a class object of the model for simulation
          parameters:  either a dict or a list.
            If it is a dict, the keys are the arguments to the class constructions and the
              values are a list of values that argument should take.
              e.g.: {'arg1':[1,2,3],'arg2':[1,2,3]}  will produce 9 parameter points.
            If it is a list, where each element of the list is a dict
            """
        self.parameters = None

        if not isinstance(name, str):
            raise MolnsUtilException("name not specified")
        self.name = name
        if not inspect.isclass(model_class):
            raise MolnsUtilException("model_class not a class")
        self.model_class = cloudpickle.dumps(model_class)
        # Set the Ipython.parallel client
        self.num_engines = num_engines
        self._update_client(client)
        if not self.load_state(ignore_model_mismatch=ignore_model_mismatch):
            # State not found, set defaults
            self.number_of_trajectories = 0
            self.seed_base = self.generate_seed_base()
            self.storage_mode = None
            self.result_list = {}
            self.running_MapReduceTask = None
            self.running_SimulationTask = None
            self.reduced_results = None
            self.mapped_results = None
            self.number_of_results = 0
            self.step1_complete = False
            self.step2_complete = False
            self.step3_complete = False
            self.mapper_fn = None
            self.aggregator_fn  = None
            self.reducer_fn = None

        if self.parameters is None:
            # process the parameters
            if type(parameters) is type({}):
                vals = []
                keys = []
                for key, value in parameters.items():
                     keys.append(key)
                     vals.append(value)
                pspace=itertools.product(*vals)

                paramsets = []

                for p in pspace:
                    pset = {}
                    for i,val in enumerate(p):
                        pset[keys[i]] = val
                    paramsets.append(pset)

                self.parameters = paramsets
            elif type(parameters) is type([]):
                self.parameters = parameters
            else:
                raise MolnsUtilException("parameters must be a dict.")


    def _determine_chunk_size(self, number_of_trajectories):
        """ Determine a optimal chunk size. """
        num_params = len(self.parameters)
        if num_params >= self.num_engines:
            return number_of_trajectories
        return int(max(1, math.ceil(number_of_trajectories*num_params/float(self.num_engines))))

    def run_reducer(self, reducer):
        """ Inside the run() function, apply the reducer to all of the map'ped-aggregated result values. """
        ret = ParameterSweepResultList()
        for param_set_id, param in enumerate(self.parameters):
            ret.append(ParameterSweepResult(reducer(self.mapped_results[param_set_id], parameters=param), parameters=param))
        return ret
    #--------------------------



class ParameterSweepResult():
    """ Result object to encapsulate the results of a parameter sweep at a particular parameter point along with the parameters used to compute it.  
    
    The object has two member variables, 'result' which contains the MapReduce output and 'parameters' which is a dict containing the parameters used to compute the simulation trajectories.
    """
    def __init__(self, result, parameters):
        self.result = result
        self.parameters = parameters

    def __str__(self):
        return "{0} => {1}".format(self.parameters, self.result)

class ParameterSweepResultList(list):
    """ List encapsulating the ParameterSweepResult objects. """
    def __str__(self):
        l = []
        for i in self:
            l.append(str(i))
        return "[{0}]".format(", ".join(l))



if __name__ == '__main__':

    ga = PersistentStorage()
    #print ga.list_buckets()
    ga.put('testtest.pyb',"fdkjshfkjdshfjdhsfkjhsdkjfhdskjf")
    print ga.get('testtest.pyb')
    ga.delete('testtest.pyb')
    ga.list()
    ga.put('file1', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.put('file2', "fdlsfjdkls")
    ga.delete_all()
