import sys

from src.main.read import FileReader
from src.test.main_test import PySparkTestCase


class ReaderTestCase(PySparkTestCase):

    def test_read(self):
        reader = FileReader(self.spark)
        df = reader.read_txt_file("/Users/gejohn2001/PycharmProjects/de-assessment/resources/in.tsv", "\t")
        print(df.count())
        self.assertEqual(df.count(), 21)