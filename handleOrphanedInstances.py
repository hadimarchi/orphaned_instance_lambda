import psycopg2
from configparser import SafeConfigParser


class orphanedInstanceHandler:
    def __init__(self):
        self.get_config()
        self.get_hyp3_connection()

    def get_config(self):
        self.config = SafeConfigParser()
        self.config.read('./config.cfg')

    def get_hyp3_connection(self):
        hyp3_db = self.db_connection_string(db='hyp3-db')
        self.hyp3_db = psycopg2.connect(hyp3_db)
        self.hyp3_db.autocommit = True

    def db_connection_string(self, db):
        connection_string = \
            "host='" + self.config.get(db, 'host') + "' " + \
            "dbname='" + self.config.get(db, 'db') + "' " + \
            "user='" + self.config.get(db, 'user') + "' " + \
            "password='" + self.config.get(db, 'pass') + "'"

        return connection_string


def handleOrphanedInstances():
    orphaned_instance_handler = orphanedInstanceHandler()


def lambda_handler(event, context):
    return handleOrphanedInstances()


if __name__ == "__main__":
    lambda_handler(1, 2)
