from pyspark.sql import SparkSession

class SessionProvider:
    def get_session(self):
        return SparkSession.builder.getOrCreate()