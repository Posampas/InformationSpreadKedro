import unittest
from src.informationspread.pipelines.data_processing.nodes import remove_RT

import pandas as pd

test_frame = pd.DataFrame({id:["1","2","3","4"], "text":["RT text","text","text","RT text"]})

class TestNodes(unittest.TestCase):
    def test_can_import(self):
        func = remove_RT
        self.assertIsNotNone(func)
    

    def test_should_return_data_frame(self):
        result = remove_RT(pd.DataFrame(test_frame))
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
    
    def test_should_remove_retwitts(self):
        result = remove_RT(test_frame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result["text"].iloc[0], test_frame["text"][1])
        self.assertEqual(result["text"].iloc[1], test_frame["text"][2])

    def test_should_not_remove_any_columns(self):
        result = remove_RT(test_frame)
        self.assertListEqual(result.columns.to_list(), test_frame.columns.to_list())

    def test_should_throw_custom_msg_excetpion_when_no_text_column(self):
        with self.assertRaises(RuntimeError) as contex:
            remove_RT(pd.DataFrame({"id" :[1,2], "location" :["Wwa","Wwa"]}))
        self.assertTrue("Column \"text\" has to be present in the input frame" in str(contex.exception))
        
