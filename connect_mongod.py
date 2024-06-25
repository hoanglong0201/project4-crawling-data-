import pymongo

class PipeLine(object):
    def __init__(self):
        self.conn = pymongo.MongoClient(
                'localhost',
                27017
        )
        db = self.conn['jobs']
        self.collection = db['jobs_data']