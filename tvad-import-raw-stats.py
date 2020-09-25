
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
    # statistic collection
    stats = db.statistics_raw

    # inserted records
    inserted = 0
    # starting offset of downloaded statistics
    offset = 0

    # flag
    run = True
    # retrieve all statistics
    while run :
        # set counter
        counter = 0
        # print logging info
        logging.info('Retrieving statistics with starting ID offset {}'.format(offset))
        # get patient statistics
        p_stats = requests.get('http://tvassistdem-backend.istc.cnr.it/statistics/{}/stream'.format(offset))
        if p_stats.status_code != 200 :
            # something went wrong
            logging.error("Can't import statistics for patient with offset ID {}".format(offset))
            run = False
        else :

            # check statistic data
            for stat in p_stats.json():

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
                    "current_app_package_name" : stat['currentAppPackageName'],
                    "current_app_version" : stat['currentAppVersion'],
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
                    logging.debug('Importing {} {} {} {}'.format(stat['id'],
                                                              stat['userId'],
                                                              stat['type'].encode('utf-8').strip(),
                                                              stat['action'].encode('utf-8').strip()))
                    # new record inserted
                    inserted += 1
                else :

                    # statistic record already imported
                    logging.debug('Statistic record with ID {} already imported'.format(stat['id']))

                # close statistic cursor
                s_cursor.close()

        # stop when no more statistic has been inserted
        if counter == 0 :
            # stop retrieving statistics for the current patient
            run = False

    logging.info('A total number of {} records have been inserted'.format(inserted))
