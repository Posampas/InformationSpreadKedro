from unittest import TestCase
from src.informationspread.pipelines.data_sience.nodes import db_scan_node
from src.informationspread.pipelines.data_sience.nodes import remove_non_polish_locations
import pandas as pd



class TestObjects:
    def __init__(self) -> None:
        
        self.test_frame = {
                    "country": ["Polska","Polska","Polska", "Czad" ],
                    "city": ["Kraków","Warszawa", "Wrocław", "Czad"],
                    "lat": [50.046910, 52.231924, 37.958746,15.613414],
                    "lon": [19.997064, 21.006726, -76.758021,19.015617],
                    "user_id": [1,1,2,3]
                } 


class DbScanNodeTest(TestCase):
    def setUp(self) -> None:
        self.test_objects = TestObjects()
        self.inputFrame = pd.DataFrame(self.test_objects.test_frame)
        self.assertIsNotNone(self.inputFrame)

    def test_should_import(self):
        pass

class RemoveNonPolishLocations(TestCase):

    def setUp(self) -> None:
       self.test_objects = TestObjects()
       self.test_frame = pd.DataFrame(self.test_objects.test_frame)

    def test_should_return_dataframe(self):
        result = remove_non_polish_locations(self.test_frame)
        self.assertIsInstance(result,pd.DataFrame)
        self.assertEqual(len(result), 3)
        countries_present_in_frame = result['country'].unique()
        print(countries_present_in_frame)
        self.assertEqual(len(countries_present_in_frame), 1)
        self.assertEqual(countries_present_in_frame[0] , "Polska")
