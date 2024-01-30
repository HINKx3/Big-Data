import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import ingest
import transform
import persist
import logging
import logging.config


class Pipeline:
    logging.config.fileConfig("resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark)
            df = ingest_process.ingest_data()
            df.show()
            tranform_process = transform.Transform(self.spark)
            transformed_df = tranform_process.transform_data(df)
            transformed_df.show()
            persist_process = persist.Persist(self.spark)
            persist_process.persist_data(transformed_df)
            logging.info('run_pipeline method ended')
        except Exception as exp:
            logging.error("An error occured while running the pipeline > " +str(exp) )
            # send email notification
            # log error to database
            sys.exit(1)

        return

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("my first spark app")\
            .enableHiveSupport().getOrCreate()

    def create_hive_table(self):
        self.spark.sql("create database if not exists coursedb")
        self.spark.sql("create table if not exists coursedb.course_table (course_id string,course_name string,author_name string,no_of_reviews string)")
        self.spark.sql("insert into coursedb.course_table VALUES (1,'Java','Hink',45)")
        self.spark.sql("insert into coursedb.course_table VALUES (2,'Java','HinkSkill',56)")
        self.spark.sql("insert into coursedb.course_table VALUES (3,'Big Data','hink',100)")
        self.spark.sql("insert into coursedb.course_table VALUES (4,'Linux','hink',100)")
        self.spark.sql("insert into coursedb.course_table VALUES (5,'Microservices','hink',100)")
        self.spark.sql("insert into coursedb.course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into coursedb.course_table VALUES (7,'Python','Hink','')")
        self.spark.sql("insert into coursedb.course_table VALUES (8,'CMS','hink',56)")
        self.spark.sql("insert into coursedb.course_table VALUES (9,'Dot Net','HinkSkill',34)")
        self.spark.sql("insert into coursedb.course_table VALUES (10,'Ansible','Hink',123)")
        self.spark.sql("insert into coursedb.course_table VALUES (11,'Jenkins','hink',32)")
        self.spark.sql("insert into coursedb.course_table VALUES (12,'Chef','Hink',121)")
        self.spark.sql("insert into coursedb.course_table VALUES (13,'Go Lang','',105)")
        #Treat empty strings as null
        self.spark.sql("alter table coursedb.course_table set tblproperties('serialization.null.format'='')")


if __name__ == '__main__':
    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.create_hive_table()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')


