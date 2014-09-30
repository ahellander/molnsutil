""" 
  Utility module for MOLNs^2. 
  
  molnsutil contains implementations of a persisitent storage API for 
  staging objects to an Object Store in the clouds supported by MOLNs^2. 
  This can be used in MOLNs^2 to write variables that are presistent
  between sessions, provides a convenetient way to get data out of the
  system, and it also provides a means during parallel computations to 
  stage data so that it is visible to all compute engines, in contrast
  to using the local scratch space on the engines.

  molnsutil also contains parallel implementations of common Monte Carlo computational
  workflows, such as the generaion of ensembles and esitmation of moments.
  
"""


import boto
import boto.ec2
from os import environ
import logging
from boto.s3.connection import S3Connection
logging.basicConfig(filename="boto.log", level=logging.DEBUG)
from boto.s3.key import Key
import uuid

import swiftclient.client
import IPython.parallel
import uuid
from IPython.display import HTML, Javascript, display

class MolnsUtilStorageException(Exception):
    pass


try:
    import dill as pickle
except:
    import pickle

import json

#     s3.json is a JSON file that contains the follwing info:
#
#     'aws_access_key_id' : AWS access key
#     'aws_secret_access_key' : AWS private key
#   s3.json needs to be created and put in .molns/s3.json in the root of the home directory. 

import os
with open(os.environ['HOME']+'/.molns/s3.json','r') as fh:
    s3config = json.loads(fh.read())


class LocalStorage():
    """ This class provides an abstraction for storing and reading objects on/from
        the ephemeral storage. """
    
    def __init__(self):
	self.folder_name = "/home/ubuntu/localarea"
	
    def put(self, filename, data):
        with open(self.folder_name+"/"+filename,'wb') as fh:
            fh.write(pickle.dumps(data))

    def get(self, filename):
        with open(self.folder_name+"/"+filename, 'rb') as fh:
            data = pickle.load(fh)
        return data

    def delete(self,filename):
        os.remove(self.folder_name+"/"+filename)

class SharedStorage():
    """ This class provides an abstraction for storing and reading objects on/from
        the sshfs mounted storage on the controller. """
    
    def __init__(self):
	self.folder_name = "/home/ubuntu/shared"
	
    def put(self, filename, data):
        with open(self.folder_name+"/"+filename,'wb') as fh:
            fh.write(pickle.dumps(data))

    def get(self, filename):
        with open(self.folder_name+"/"+filename, 'rb') as fh:
            data = pickle.load(fh)
        return data

    def delete(self,filename):
        os.remove(self.folder_name+"/"+filename)


class S3Provider():
    def __init__(self, bucket_name):
        self.connection = S3Connection(is_secure=False,
                                 calling_format='boto.s3.connection.OrdinaryCallingFormat',
                                 **s3config['credentials']
                                 )
        self.set_bucket(bucket_name)
    
    def set_bucket(self,bucket_name=None):
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

    def create_bucket(self,bucket_name):
        return self.connection.create_bucket(bucket_name)

    def put(self, name, data):
        k = Key(self.bucket)
        if not k:
            raise MolnsUtilStorageException("Could not obtain key in the global store. ")
        k.key = name
        try:
            num_bytes = k.set_contents_from_string(data)
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
        """ List all bucket in this provider. """

    def put(self, object_name, data):
        self.connection.put_object(self.bucket_name, object_name, data)

    def get(self, object_name, validate=False):
        (response, obj) = self.connection.get_object(self.bucket_name, object_name)
        return obj

    def delete(self, object_name):
        self.connection.delete_object(self.bucket_name, object_name)

    def delete_all(self):
        print self.connection.head_container(self.bucket_name)

    def list(self):
        """ TODO: implement. """

    def close(self):
        self.connection.close()

    def __del__(self):
        self.close()


class PersistentStorage():
    """
       Provides an abstaction for interacting with the Object Stores
       of the supported clouds.
    """

    def __init__(self, bucket_name=None):
        #print s3config['credentials']
        
        if bucket_name is None:
            # try reading it from the config file
            try:
                bucket_name = s3config['bucket_name']
            except:
                pass
    
        if s3config['provider_type'] == 'EC2':
            self.provider = S3Provider(bucket_name)
        # self.provider = S3Provider()
        elif s3config['provider_type'] == 'OpenStack':
            self.provider = SwiftProvider(bucket_name)
        else:
            raise MolnsUtilStorageException("Unknown provider type.")
        

    def list_buckets(self):
        all_buckets=self.provider.get_all_buckets()
        buckets = []
        for bucket in all_buckets:
            buckets.append(bucket.name)
        return buckets

    def set_bucket(self,bucket_name=None):
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
        self.provider.put(name, pickle.dumps(data))
    
    
    def get(self, name, validate=False):
        return pickle.loads(self.provider.get(name, validate))
    
    def delete(self, name):
        """ Delete an object. """
        self.provider.delete(name)
    
    def list(self):
        """ List all containers. """
        return self.provider.list()

    def delete_all(self):
        """ Delete all objects in the global storage area. """
        self.provider.delete_all()


def run_ensemble(model,nt,storage_mode="Shared"):
    """ Generates an ensemble consisting of number_of_trajectories realizations by
        running pyurdme nt number of times. The resulting pyurdme result objects
        are serialized and written to one of the MOLNs storage locations, each
        assigned a random filename. The default behavior is to write the
        files to the Shared storage location (global non-persistent). Optionally, files can be
        written to the Object Store (global persistent), storage_model="Persistent"
        
        Returns: a list of filenames for the serialized result objects.
        
        """
    
    import pyurdme
    from pyurdme.nsmsolver import NSMSolver
    import sys
    import uuid
    from molnsutil import PersistentStorage, LocalStorage, SharedStorage
    
    if storage_mode=="Shared":
        storage  = SharedStorage()
    elif storage_mode=="Persistent":
        storage = PersistentStorage()
    # Run the solver
    solver = NSMSolver(model)
    seed_base = int(uuid.uuid4())
    filenames = []
    for i in range(nt):
        try:
            result = solver.run(seed=seed_base+i)
            filename = str(uuid.uuid1())
            storage.put(filename, result)
            filenames.append(filename)
        except:
            raise
    
    return filenames


def add(a, b=None):
    if b==None:
        return a
    return a+b

def map_and_reduce(results, mapper, reducer, cache_results=False):
    """ Reduces a list of results by applying the map function 'mapper'.
        When this function is applied on an engine, it will first
        look for the result object in the local ephemeral storage (cache),
        then in the Shared area (global non-persisitent), then in the
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
    
    num_processed=0
    res = None
    result = None
    for i,filename in enumerate(results):
        try:
            result = ls.get(filename)
        except:
            pass
        
        if result is None:
            try:
                result = ss.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except:
                pass
        
        if result is None:
            try:
                result = ps.get(filename)
                if cache_results:
                    ls.put(filename, result)
            except:
                pass
        
        try:
            mapres = mapper(result)
            res = reducer(mapres, res)
            num_processed +=1
        except Exception as e:
            raise
    return {'result':res, 'num_sucessful':num_processed, 'num_failed':len(results)-num_processed}

class DistributedEnsemble():
    """ A distributed ensemble. """
    
    def __init__(self, name=None, model_class=None, model=None, client=None, number_of_realizations=1, persistent=False):
        """ hjhkjhjk """
        self.model_class = model_class
        self.number_of_realizations = number_of_realizations
        self.persistent = persistent
        
        # A chunk list
        self.result_list = []
        
        self.update_client(client)
    
    def update_client(self, client=None):
        if client is None:
            self.c = IPython.parallel.Client()
        else:
            self.c = client
        self.c[:].use_dill()
        self.dv = self.c[:]
        self.lv = self.c.load_balanced_view()
    
    def rebalance_chunk_list(self):
        """ It seems like it can be necessary to be able to rebalance the chunk list if
            the number of engines change. Like if you suddenly have more engines than chunks, you
            want to create more chunks. """
    
    def add_realizations(self, number_of_realizations=1, chunk_size=1, blocking=True, progress_bar=True, storage_mode="Shared"):
        """ Add a number of realizations to the ensemble. """
        model = self.model_class()
        num_chunks = int(number_of_realizations/chunk_size)
        chunks = [chunk_size]*(num_chunks-1)
        chunks.append(number_of_realizations-chunk_size*(num_chunks-1))
        results  = self.lv.map_async(run_ensemble,[model]*num_chunks,chunks,[storage_mode]*num_chunks)
        
        if progress_bar:
            # This should be factored out somehow.
            divid = str(uuid.uuid4())
            pb = HTML(
                      """
                          <div style="border: 1px solid black; width:500px">
                          <div id="%s" style="background-color:blue; width:0%%">&nbsp;</div>
                          </div>
                          """ % divid)
            display(pb)
        
        # We process the results as they arrive.
        for i,r in enumerate(results):
            self.result_list.append(r)
            if progress_bar:
                display(Javascript("$('div#%s').width('%f%%')" % (divid, 100.0*(i+1)/len(results))))
        
        
        return {'wall_time':results.wall_time}
    
    def _determine_chunk_size(self):
        """ Determine a good chunk size in some clever way. """
        num_chunks = len(self.c.ids())
        return num_chunks
    
    def save():
        """ Save the data in the object store. """
    
    def _clear_cache(self):
        """ Remove all cached result objects on the engines. """
    
    def delete_realizations(self):
        """ Delete realizations from the object store. """
    
    
    def mean(self, mapper=None, number_of_realizations=None, blocking=True, interactive=False, cache_results=False):
        """ Compute the mean of the function g(X) based on number_of_realizations realizations
            in the ensemble. It has to make sense to say g(result1)+g(result2). """
        
        num_chunks = len(self.c.ids)
        nc = len(self.result_list)
        # Now map the postprocessing routine using the view that matches the file locations on the engines.
        pr = self.c[:].map_async(map_and_reduce, self.result_list, [mapper]*nc,[add]*nc,[cache_results]*nc)
        #pr.wait()
        res = {}
        num_sucessful=0
        for i,p in enumerate(pr):
            try:
                if i==0:
                    meanx = p['result']/p['num_sucessful']
                else:
                    meanx = meanx+p['result']/p['num_sucessful']
                num_sucessful+=1
                if interactive:
                    print meanx/num_sucessful
            except Exception as e:
                raise
        
        res['mean'] = meanx/num_sucessful
        res['wall_time']=pr.wall_time
        #res['variance'] =
        #res['confidence_interval'] =
        return res
    
    
    def mean_variance(self, g=None, number_of_realizations=None):
        """ Compute the variance (second order central moment) of the function g(X) based on number_of_realizations realizations
            in the ensemble. """
    
    def moment(self, g=None, order=1, number_of_realizations=None):
        """ Compute the moment of order 'order' of g(X), using number_of_realizations
            realizations in the ensemble. """
    
    def histogram_density(self, g=None, number_of_realizations=None):
        """ Estimate the probability density function of g(X) based on number_of_realizations realizations
            in the ensemble. """



class ParameterSweep():
    """ Making parameter sweeps on distributed compute systems easier. """
    def __init__(self, model_class, parameters, number_of_realizations=1, persistent_data=False, persistent_data_delete_timeout='48hr', mapper=None, reducer=None):
        pass


class ParameterSweepResult():
    """TODO"""
    pass

class ParameterSweepResultList():
    """TODO"""
    pass





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
