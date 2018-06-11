import psycopg2
import boto3
from configparser import SafeConfigParser


class orphanedInstanceHandler:
    def __init__(self):
        self.get_config()
        self.get_hyp3_connection()
        self.get_sql()
        self.get_ec2_client()

    def get_config(self):
        self.config = SafeConfigParser()
        self.config.read('./config.cfg')

    def get_hyp3_connection(self):
        hyp3_db = self.db_connection_string(db='hyp3-db')
        self.hyp3_db = psycopg2.connect(hyp3_db)
        self.hyp3_db.autocommit = True

    def get_sql(self):
        self.get_running_instances_sql = self.config.get("sql",
                                                         "get_running_instances_sql")

    def get_ec2_client(self):
        sts_client = boto3.client('sts', region_name='us-east-1')
        temp_credentials = sts_client.assume_role(
            RoleArn=self.config.get('full-access-role', 'arn'),
            RoleSessionName=self.config.get('full-access-role', 'session_name'))
        self.ec2_client = boto3.client('ec2', region_name='us-east-1',
                                       aws_access_key_id=(
                                           temp_credentials["Credentials"]["AccessKeyId"]),
                                       aws_secret_access_key=(
                                           temp_credentials["Credentials"]["SecretAccessKey"]),
                                       aws_session_token=(
                                           temp_credentials["Credentials"]["SessionToken"]))

    def db_connection_string(self, db):
        connection_string = (
            "host='" + self.config.get(db, 'host') + "' " +
            "dbname='" + self.config.get(db, 'db') + "' " +
            "user='" + self.config.get(db, 'user') + "' " +
            "password='" + self.config.get(db, 'pass') + "'")

        return connection_string

    def get_orphaned_instances(self):
        self.get_live_instances_from_hyp3()
        self.get_live_instances_from_aws()
        self.orphaned_instances = list()
        for hyp3_instance_id in self.hyp3_live_instance_ids:
            if hyp3_instance_id not in self.aws_live_instance_ids:
                self.orphaned_instances.append(hyp3_instance_id)

    def get_live_instances_from_hyp3(self):
        hyp3_live_instances = self.do_hyp3_sql(
            self.get_running_instances_sql)
        self.hyp3_live_instance_ids = [
            instance[0] for instance in hyp3_live_instances]

    def get_live_instances_from_aws(self):
        hyp3_live_instances = self.ec2_client.describe_instances()[
            "Reservations"]
        self.aws_live_instance_ids = list()
        for instance in hyp3_live_instances:
            if instance["Instances"][0]["State"]["Name"] == 'running':
                self.aws_live_instance_ids.append(
                    instance["Instances"][0]["InstanceId"])

    def shutdownOrphans(self):
        shutdown_sql = self.config.get('sql', 'give_orphans_shutdown_time')
        finish_records_sql = self.config.get('sql',
                                             'give_orphan_instance_records_end_time')
        self.do_hyp3_sql(shutdown_sql, vals={
                         'instance_ids': self.orphaned_instances})
        self.do_hyp3_sql(finish_records_sql, vals={
                         'instance_ids': self.orphaned_instances})

    def do_hyp3_sql(self, sql, vals=None):
        cur = self.hyp3_db.cursor()
        cur.execute(sql, vals) if vals else cur.execute(sql)

        try:
            res = cur.fetchall()
        except Exception:
            return
        else:
            return res
        finally:
            self.hyp3_db.commit()
            cur.close()


def handleOrphanedInstances():
    orphaned_instance_handler = orphanedInstanceHandler()
    orphaned_instance_handler.get_orphaned_instances()
    orphaned_instance_handler.shutdownOrphans()
    return len(orphaned_instance_handler.orphaned_instances)


def lambda_handler(event, context):
    return handleOrphanedInstances()


if __name__ == "__main__":
    lambda_handler(1, 2)
