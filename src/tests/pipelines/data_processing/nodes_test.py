import unittest
from src.informationspread.pipelines.data_processing.nodes import remove_RT
from src.informationspread.pipelines.data_processing.nodes import join_user_text
from src.informationspread.pipelines.data_processing.nodes import remove_non_polish_tweets
from src.informationspread.pipelines.data_processing.nodes import remove_regex_from_text
from src.informationspread.pipelines.data_processing.nodes import remove_non_ascii_chars
from src.informationspread.pipelines.data_processing.nodes import convet_text_to_base_form
import pandas as pd


from src.informationspread.processors.clarinService import ClarinService
from src.informationspread.processors.xml_parser import XmlParser 
from unittest import mock

test_frame = pd.DataFrame({id: ["1", "2", "3", "4"], "text": [
                          "RT text", "text", "text", "RT text"]})


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
            remove_RT(pd.DataFrame({"id": [1, 2], "location": ["Wwa", "Wwa"]}))
        self.assertTrue(
            "Column \"text\" has to be present in the input frame" in str(contex.exception))


class TestTextJoiningNode(unittest.TestCase):

    def setUp(self) -> None:
        self.minimal_allowed = pd.DataFrame({"user_id": [1], "text": ["asdf"]})
        self.only_one_user = pd.DataFrame(
            {"user_id": [1, 1], "text": ["firstTwitt", "Second_twitt"]})
        self.more_then_one_user = pd.DataFrame(
            {"user_id": [1, 2, 1, 3, 2], "text": ["u_1", "u_2", "u_1", "u_3", "u_2"]})

    def test_should_import(self):
        func = join_user_text

    def test_should_return_DataFrame(self):
        result = join_user_text(self.minimal_allowed)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_should_join_user_data(self):
        result = join_user_text(self.only_one_user)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0]["text"], "firstTwitt\nSecond_twitt")

        result = join_user_text(self.more_then_one_user)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[result["user_id"] == 1]["text"].iloc[0], "u_1\nu_1")
        self.assertEqual(result[result["user_id"] == 2]["text"].iloc[0], "u_2\nu_2")
        self.assertEqual(result[result["user_id"] == 3]["text"].iloc[0], "u_3")

    def test_should_throw_expection_when_no_column_user_id_and_text(self):
        with self.assertRaises(RuntimeError) as context:
            join_user_text(pd.DataFrame({"lang": ["pl"]}))
        self.assertTrue("Column \"text\" has to be present in the input frame" in str(
            context.exception))
        with self.assertRaises(RuntimeError) as context:
            join_user_text(pd.DataFrame({"lang": ["pl"], "text": ["dfa"]}))
        self.assertTrue("Column \"user_id\" has to be present in the input frame" in str(
            context.exception))
        join_user_text(self.minimal_allowed)


class TestRemoveNonPolishTwitts(unittest.TestCase):

    def setUp(self) -> None:
        self.minimal_acceptable = pd.DataFrame({"lang": ["pl", "pl"], "id": [1, 2]})

    def test_can_import(self):
        function = remove_non_polish_tweets
        self.assertIsNotNone(function)

    def test_should_return_data_frame(self):
        result = remove_non_polish_tweets(self.minimal_acceptable)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_should_throw_exception_when_no_column_lang(self):
        with self.assertRaises(RuntimeError) as context:
            remove_non_polish_tweets(pd.DataFrame({"id": [1]}))
        self.assertTrue(
            "Column \"lang\" has to be present in the input frame" in str(context.exception))

    def test_should_not_raise_exception_when_column_lang_is_avaliablae(self):
        function = remove_non_polish_tweets(self.minimal_acceptable)

    def test_should_not_remove_any_twitt_if_only_polish_twitt_avaliable(self):
        result = remove_non_polish_tweets(
            pd.DataFrame({"lang": ["pl", "pl"], "id": [1, 2]}))
        self.assertEqual(len(result), 2)

    def test_should_not_remove_non_polish_twitts(self):
        result = remove_non_polish_tweets(
            pd.DataFrame({"lang": ["pl", "pl", "ang"], "id": [1, 2, 3]}))
        self.assertEqual(len(result), 2)


class TestRemoveReqexNode(unittest.TestCase):
    def test_should_import(self):
        function = remove_regex_from_text
        self.assertIsNotNone(function)

    def test_should_accept_data_frame_that_contaion_column_text_and_string_regex_and_return_dataframe(self):
        result = remove_regex_from_text(pd.DataFrame({"text": ["text"]}), "regex")
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_should_throw_exception_when_column_text_is_not_preseent(self):

        with self.assertRaises(RuntimeError) as context:
            remove_regex_from_text(pd.DataFrame({"id": [1, 2]}), "regex")

        self.assertTrue(
            "Column \"text\" has to be present in the input frame" in str(context.exception))

    def test_should_remove_regex_from_text(self):
        text = "this is acctual text"
        input_frame = pd.DataFrame(
            {"text": ["@username1 @username2 " + text + " @usernam_e"]})
        result = remove_regex_from_text(input_frame, "@[A-Za-z0-9_]+")
        self.assertEqual(result['text'].iloc[0], text)

    def test_should_remove_links_form_text(self):
        regex = 'https://t.co/[A-Za-z0-9]+'
        text = "this is acctual text"
        input_frame = pd.DataFrame(
            {"text": ["https://t.co/XpfTeqxger " + text + " https://t.co/XSGlvprEb9"]})
        result = remove_regex_from_text(input_frame, regex)
        print(result['text'].iloc[0])
        self.assertEqual(result['text'].iloc[0], text)

    def test_should_remove_emojjis(self):
        regex = "([\U0001F1E0-\U0001F1FF]|[\U0001F300-\U0001F5FF]|[\U0001F600-\U0001F64F]|[\U0001F680-\U0001F6FF]|[\U0001F700-\U0001F77F]|[\U0001F780-\U0001F7FF]|[\U0001F800-\U0001F8FF]|[\U0001F900-\U0001F9FF]|[\U0001FA00-\U0001FA6F]|[\U0001FA70-\U0001FAFF]|[\U00002702-\U000027B0]|[\U000024C2-\U0001F251])"
        text = "this is acctual text"
        input_frame = pd.DataFrame({"text": [" 😂🙂{}🤦💪".format(text)]})
        result = remove_regex_from_text(input_frame, regex)
        print(result['text'].iloc[0])
        self.assertEqual(result['text'].iloc[0], text)

    def test_should_throw_exception_when_regex_is_not_string(self):
        with self.assertRaises(RuntimeError) as context:
            remove_regex_from_text(pd.DataFrame({"text": ["1"]}), 1)

        self.assertTrue(
            "Regex must be string and be parsable to regex")


class TestRemoveNonAsciChar(unittest.TestCase):

    def test_remove_non_asci_char(self):
        expected_text = "acctual_text"
        input_frame = pd.DataFrame({"text": [" 😂🙂{}🤦💪".format(expected_text)]})
        result = remove_non_ascii_chars(input_frame)
        self.assertEqual(result['text'].iloc[0], expected_text)


def mocked_clarinServiceRun(*args, **kwargs):
    return ["<xml></xml>","<xml></xml>"]
def mocked_xml_parser(*args, **kwagrs):
    return "Warszawa"

class TestConvertToBaseFormUnsingClarinService(unittest.TestCase):

    def setUp(self) -> None:
        self.input = pd.DataFrame({"id": [1, 2], "text": ["Jest dobrze Warszawa", "Jest źle"]})

    def test_should_import_function(self):
        func = convet_text_to_base_form
        self.assertIsNotNone(func)

    def test_should_throw_excption_when_no_column_text(self):
        with self.assertRaises(RuntimeError) as context:
            convet_text_to_base_form(pd.DataFrame({"id": [1, 2]}))

        self.assertTrue(
            "Column \"text\" has to be present in the input frame" in str(context.exception))

    def test_should_throw_excption_when_no_column_id(self):
        with self.assertRaises(RuntimeError) as context:
            convet_text_to_base_form(pd.DataFrame(
                {"text": ["Jest dobrze", "Jest źle"]}))

        self.assertTrue(
            "Column \"id\" has to be present in the input frame" in str(context.exception))

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun)  
    def test_should_not_return_None(self):
        result = convet_text_to_base_form(self.input)
        self.assertIsNotNone(result)

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun)  
    def test_should_return_data_frame_with_columns_id_and_text(self):
        result = convet_text_to_base_form(self.input)
        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = ["id", "text"]
        self.assertListEqual(expected_columns, result.columns.to_list()) 

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun) 
    @mock.patch.object(XmlParser,'extractBaseFormOfWordsThatHasGeoAnnotation', new = mocked_xml_parser) 
    def test_text_should_be_transformed_to_base_form(self):
        expected_text = "Warszawa;Warszawa"
        #input text des not really matters coz other valuse are returnred from mecked functions
        result = convet_text_to_base_form(self.input)
        print(result)
        self.assertEqual(result.iloc[0]['text'], expected_text)
    
