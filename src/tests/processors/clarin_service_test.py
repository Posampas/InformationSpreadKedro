
import tempfile
from unittest import mock, TestCase
from os import path
from src.informationspread.processors.clarinService import ClarinService

def mocked_uploadfile(*args,**kwargs):
    print("Hello from mocked uplad")
    return 1 

def mocked_download_file(*args, **kwargs):
    print("Hello from mocked download")
    return 1

class TestClarinService(TestCase):

    def test_temporary_directory_should_be_removed(self):
        
        with tempfile.TemporaryDirectory() as tmpDir:
            print(tmpDir)

    def test_should_accept_text_as_constructor_parameter(self):
        text = "Text"
        service = ClarinService(text)
        self.assertIsNotNone(service)
        self.assertEqual(text, service.text)

    def test_run_function_shoud_return_string(self):
        text = "Text"
        service = ClarinService(text)
        result = service.run()
        self.assertIsInstance(result, str)

    # @mock.patch('lpmn_client.upload_file', side_effect=mocked_uploadfile)
    # @mock.patch('lpmn_client.download_file' , side_effect=mocked_download_file)
    # def test_should_call_uplad_file_with_path_to_created_tmp_file_with_tweet_text(self, mock, dowload):
        
    def test_should_delete_files_created_in_temporary_file(self):
        path_to_tmp_dir = None
        try: 
            with tempfile.TemporaryDirectory() as tempDir:
                path_to_tmp_dir = tempDir
                path_to_tmp_file = tempfile.NamedTemporaryFile(dir=path_to_tmp_dir)
                self._raise_exception() 
        except:
            pass
        print(path_to_tmp_dir)
        print(path_to_tmp_file.name)
        self.assertFalse(path.exists(path_to_tmp_dir))
        self.assertFalse(path.exists(path_to_tmp_file.name))

    def _raise_exception(self):
        raise RuntimeError() 

    def test_should_not_split_when_text_is_shorter_than_treashold(self):
        service = ClarinService("dd")
        text = "This is test, String text"
        expected = ["This is test, String text"]
        min_len = 100
        result = service._divide_text_into_chunks(text, min_len)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))
        self.assertListEqual(expected, result)
    
    def test_should_split_text_when_longer_than_treshold(self):
        service = ClarinService("dd")
        text = "This is test. String text"
        expected = ["This is test.","String text"]
        min_len= 10
        result = service._divide_text_into_chunks(text, min_len)
        print(result)
        self.assertEqual(2, len(result))
        self.assertListEqual(expected, result)         

    def test_should_split_text_into_multiple_chunks(self):
        service = ClarinService("dd")
        text = "This is test. String text. This is test." # String text. This is test. String text"
        expected = ["This is test.","String text.","This is test."]
        min_len = 15
        result = service._divide_text_into_chunks(text, min_len)
        self.assertEqual(3, len(result))
        self.assertListEqual(expected, result)         

    def test_if_there_is_no_dot_should_split_on_whitespace(self):
        service = ClarinService("dd")
        text = "This is test String text This is test"
        expected = ["This is test","String text","This is test"]
        min_len = 15 
        result = service._divide_text_into_chunks(text, min_len)
        # self.assertEqual(3, len(result))
        self.assertListEqual(expected, result)         
        