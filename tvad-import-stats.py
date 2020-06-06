
import pymongo
from pymongo import MongoClient
import requests
import logging
from datetime import datetime


if __name__ == '__main__' : 
    
    # set logging level 
    logging.getLogger().setLevel(logging.INFO)
    
    # connect to mongodb
    client = MongoClient()
    db = client.tvad
    # patient collection
    patients = db.patients
    # statistic collection
    stats = db.statistics
    
    # get a cursor on patients
    p_cursor = patients.find(no_cursor_timeout = True)
    for patient in p_cursor : 
        
        # get patient id
        patientId = patient['_id']
        
        # set start offset timestamp
        offset = 0
        # flag
        run = True
        # retrieve all patient's statistics
        while run : 
            # set counter
            counter = 0
            # print logging infor
            logging.info('Retrieving statistics for patient with ID {} offset {}'.format(patientId, offset))
            # get patient statistics 
            p_stats = requests.get('http://tvassistdem-backend.istc.cnr.it/statistics/{}/stream/{}/records'.format(patientId, offset))
            if p_stats.status_code != 200 :
                # something went wrong
                logging.error("Can't import statistics for patient with ID {}".format(patientId))
                run = False
            else : 

                # check statistic data
                for stat in p_stats.json() :
                    
                    # increment counter 
                    counter += 1
                    
                    # import statistic record received as JSON object
                    logging.debug("> Importing statistic record {}".format(stat))                
                                
                    # document to insert
                    doc = { 
                        "_id" : stat['id'], 
                        "userId" : stat['userId'],
                        "type" : stat['type'].encode('utf-8').strip(),
                        "action" : stat['action'].encode('utf-8').strip(),
                        "date" : datetime.utcfromtimestamp(stat['date']/1000).strftime('%Y-%m-%d %H:%M:%S'), # date field is a timestamp in milliseconds
                        "country" : stat['country'].encode('utf-8').strip(),
                        "deviceId" : stat['deviceId'],
                        "info" : {
                            "action" : stat['info']['action'].encode('utf-8').strip(),
                            "additional_info" : stat['info']['additional_info'].encode('utf-8').strip()
                        } 
                    }
                    
                    # update offset with the date of current date
                    offset = max(stat['id'], offset)
                    
                    # get cursor over statistics
                    s_cursor = stats.find({"_id" : stat['id']}, no_cursor_timeout = True)
                    # find record by id
                    if s_cursor.count() == 0:
                        
                        # save document into the collection
                        stats.insert_one(doc)
                        logging.debug('Importing {} {} {} {}'.format(stat['id'], 
                                                                  stat['userId'], 
                                                                  stat['type'].encode('utf-8').strip(),
                                                                  stat['action'].encode('utf-8').strip()))
                    
                    else : 
                        
                        # statistic record already imported
                        logging.debug('Statistic record with ID {} already imported'.format(stat['id']))
                        
                    # close statistic cursor
                    s_cursor.close()
                
            # check counter 
            if counter == 0 : 
                # stop retrieving statistics for the current patient
                run = False
                
    # close patient cursor
    p_cursor.close()

                
                
    
    

    
