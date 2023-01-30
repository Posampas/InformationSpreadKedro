import unittest
from src.informationspread.pipelines.ner_clarin.nodes import extract_words_with_geo_assosiation_and_convert_it_to_base_form
from src.informationspread.pipelines.ner_clarin.nodes import transform_place_names_to_geo_cordinates , _get_cordinates_for_text
import pandas as pd
import unittest.mock as mock

from src.informationspread.processors.clarinService import ClarinService
from src.informationspread.processors.xml_parser import XmlParser 

def mocked_clarinServiceRun(*args, **kwargs):
    return ["<xml></xml>","<xml></xml>"]
def mocked_xml_parser(*args, **kwagrs):
    return "Warszawa"
def mocked_xml_parser_return_empty_string(*args, **kwagrs):
    return ""

class TestConvertToBaseFormUnsingClarinService(unittest.TestCase):

    def setUp(self) -> None:
        self.input = pd.DataFrame({"user_id": [1, 2], "text": ["Jest dobrze Warszawa", "Jest źle"]})

    def test_should_import_function(self):
        func = extract_words_with_geo_assosiation_and_convert_it_to_base_form
        self.assertIsNotNone(func)

    def test_should_throw_excption_when_no_column_text(self):
        with self.assertRaises(RuntimeError) as context:
            extract_words_with_geo_assosiation_and_convert_it_to_base_form(pd.DataFrame({"id": [1, 2]}))

        self.assertTrue(
            "Column \"text\" has to be present in the input frame" in str(context.exception))

    def test_should_throw_excption_when_no_column_id(self):
        with self.assertRaises(RuntimeError) as context:
            extract_words_with_geo_assosiation_and_convert_it_to_base_form(pd.DataFrame(
                {"text": ["Jest dobrze", "Jest źle"]}))

        self.assertTrue(
            "Column \"user_id\" has to be present in the input frame" in str(context.exception))

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun)  
    def test_should_not_return_None(self):
        result = extract_words_with_geo_assosiation_and_convert_it_to_base_form(self.input)
        self.assertIsNotNone(result)

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun)  
    def test_should_return_data_frame_with_columns_id_and_text(self):
        result = extract_words_with_geo_assosiation_and_convert_it_to_base_form(self.input)
        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = ["user_id", "text"]
        self.assertListEqual(expected_columns, result.columns.to_list()) 

    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun) 
    @mock.patch.object(XmlParser,'extractBaseFormOfWordsThatHasGeoAnnotation', new = mocked_xml_parser) 
    def test_text_should_be_transformed_to_base_form(self):
        expected_text = "Warszawa;Warszawa"
        #input text des not really matters coz other valuse are returnred from mecked functions
        result = extract_words_with_geo_assosiation_and_convert_it_to_base_form(self.input)
        print(result)
        self.assertEqual(result.iloc[0]['text'], expected_text)
 
    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun) 
    @mock.patch.object(XmlParser,'extractBaseFormOfWordsThatHasGeoAnnotation', new = mocked_xml_parser_return_empty_string) 
    def test_no_geo_assosiated_text_in_response_do_not_join_that_in_fianl_string(self):
        expected_text = ""
        #input text des not really matters coz other valuse are returnred from mecked functions
        result = extract_words_with_geo_assosiation_and_convert_it_to_base_form(self.input)
        print(result)
        self.assertEqual(result.iloc[0]['text'], expected_text)


    
def mocked_get_cordinates(*args, **kwargs):
    return mocked_extract_geo_addnotations() 

def mocked_extract_geo_addnotations(*args, **kwargs):
    dict = {
        'country' : ["Polska", "Białoruś", "Polska"],
        'city' : ['Warszawa', 'Brześć', 'Warszawa'],
        'lat':[52.093751,52.637222,52.559434],
        'len' :[19.997064,23.685185,19.997064]
    }
    return pd.DataFrame(dict)

class TestTranformPlaceNameToGeoCordinates(unittest.TestCase):

    def setUp(self) -> None:
        self.input = pd.DataFrame({"user_id":[1,2,3],"text":["Warszwa;Warszawa", "Kraków","Warszwa"]})

    def test_can_import(self):
        function = transform_place_names_to_geo_cordinates
        self.assertIsNotNone(function)
    
    def test_should_throw_excption_when_no_column_id(self):
        with self.assertRaises(RuntimeError) as context:
            transform_place_names_to_geo_cordinates(pd.DataFrame(
                {"text": ["Warszawa", "Kraków"]}))

        self.assertTrue(
            "Column \"user_id\" has to be present in the input frame" in str(context.exception))

    def test_should_throw_excption_when_no_column_text(self):
        with self.assertRaises(RuntimeError) as context:
            transform_place_names_to_geo_cordinates(pd.DataFrame(
                {"user_id": [1,2], "column": ["test", "text"]}))

        self.assertTrue(
            "Column \"text\" has to be present in the input frame" in str(context.exception))

    @mock.patch('src.informationspread.pipelines.ner_clarin.nodes._get_cordinates_for_text', new = mocked_get_cordinates) 
    def test_should_iterate_transform_exery_row_same_amout_of_rows_in_input_and_output(self):
        result = transform_place_names_to_geo_cordinates(self.input)
        print(result)
        self.assertEqual(len(result), 9)
        self.assertEqual(len(result[result['user_id'] == 1]) , 3 )
        self.assertEqual(len(result[result['user_id'] == 2]) , 3 )
        self.assertEqual(len(result[result['user_id'] == 3]) , 3 )
        


    @mock.patch.object(ClarinService,'run', new = mocked_clarinServiceRun) 
    @mock.patch.object(XmlParser,'extract_geo_addnotations', new = mocked_extract_geo_addnotations) 
    def test_should_tranform_names_to_cordinates(self):
        user_id = 222
        result = _get_cordinates_for_text("Warszawa;Kraków;Poznań")
        self.assertIsNotNone(result)
        self.assertIsInstance(result,pd.DataFrame)
        self.assertEqual(len(result) ,6)