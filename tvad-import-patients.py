
import pymongo
from pymongo import MongoClient
import requests
import logging

if __name__ == '__main__' : 
    
    # set logging level 
    logging.getLogger().setLevel(logging.INFO)
    
    # connect to mongodb
    client = MongoClient()
    db = client.tvad
    collection = db.patients

    # get patient data
    result = requests.get('http://tvassistdem-backend.istc.cnr.it/patients')
    # check result
    if result.status_code != 200 : 
        # something went wrong
        raise ApiError('GET /patients/2/data {}'.format(p_data.status_code))
    
    # check patients 
    for patient in result.json() : 
        
        # document to insert
        doc = { "_id" : patient['id'], "name" : patient['name'].encode('utf-8').strip(), "surname" : patient['surname'].encode('utf-8').strip()}        
        try :
            # save document into the collection
            collection.insert_one(doc)
            logging.info('Importing {} {} {}'.format(patient['id'], patient['name'].encode('utf-8').strip(), patient['surname'].encode('utf-8').strip()))
        
        except : 
            # document already exist, do update fields
            query = {"_id": patient['id']}
            updates = { "$set" : { "name" : patient['name'].encode('utf-8').strip(), "surname" : patient['surname'].encode('utf-8').strip()} }
            
            # update database 
            collection.update_one(query, updates)
            logging.warn('Updating {} {} {}'.format(patient['id'], patient['name'].encode('utf-8').strip(), patient['surname'].encode('utf-8').strip()))
                    
    
    

    
