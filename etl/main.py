import luigi
from etl import extraction, transformation, load

class Load(luigi.Task):

    def requires(self):
        return Transformation()
    
    def run(self):
        load.execute()

    def output(self):
        pass

class Transformation(luigi.Task):

    def requires(self):
        return Extraction()
    
    def run(self):
        transformation.execute()

    def output(self):
        return luigi.LocalTarget("tmp/processed_air_quality.csv")

class Extraction(luigi.Task):

    def run(self):
        extraction.execute()
    
    def output(self):
        return luigi.LocalTarget("tmp/extracted_air_quality.zip")
