
class FileReader:
    """Read Files in any format given an input path"""
    def __init__(self,  spark):
        self.spark = spark

    def read_txt_file(self, file_path, delimiter):
       return self.spark.read.option("delimiter", delimiter).option("header", "true").csv(file_path)




