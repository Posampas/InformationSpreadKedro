import unittest
from src.informationspread.pipelines.data_processing.nodes import remove_RT
from src.informationspread.pipelines.data_processing.nodes import join_user_text

import pandas as pd

test_frame = pd.DataFrame({id:["1","2","3","4"], "text":["RT text","text","text","RT text"]})

class TestRemoveRetwittsNode(unittest.TestCase):
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
        

class TestTextJoiningNode(unittest.TestCase):

    def setUp(self) -> None:
        self.minimal_allowed = pd.DataFrame({"user_id":[1], "text":["asdf"]}) 
        self.only_one_user = pd.DataFrame({"user_id":[1, 1], "text": ["firstTwitt", "Second_twitt"]})
        self.more_then_one_user =  pd.DataFrame({"user_id":[1, 2,1,3,2], "text": ["u_1", "u_2","u_1","u_3","u_2"]}) 
    def test_should_import(self):
        func = join_user_text

    def test_should_return_DataFrame(self):
        result = join_user_text(self.minimal_allowed)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_should_join_user_data(self):
        result = join_user_text(self.only_one_user)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0]["text"],"firstTwitt\nSecond_twitt")

        result = join_user_text(self.more_then_one_user)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[result["user_id"] == 1]["text"].iloc[0],"u_1\nu_1")
        self.assertEqual(result[result["user_id"] == 2]["text"].iloc[0],"u_2\nu_2")
        self.assertEqual(result[result["user_id"] == 3]["text"].iloc[0],"u_3")

    def test_should_throw_expection_when_no_column_user_id_and_text(self):
        with self.assertRaises(RuntimeError) as context:
            join_user_text(pd.DataFrame({"lang":["pl"]}))
        self.assertTrue("Column \"text\" and \"user_id\" has to be present in the input frame" in str(context.exception))

        join_user_text(self.minimal_allowed)