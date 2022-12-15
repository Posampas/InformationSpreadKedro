
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
        min_len= 15
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

    def test_chunk(self):
        text = """
            Kłamstwa Tuska pewnie pochodziły od takich doradców jak morawiecki np
            Teraz gdy  rządzi już ładne pare lat sytuacja się nie zmieniła
            Żart? Może warto na kogoś nowego choć oddać głos?
            A co powiesz o Obajtku i cenach paliwa?
            Kiedy pan ostatnio u lekarza był?
            Czyli maszyna jest bardziej rozwinięta niż żywy organizm? No ciekawa teoria ale chyba na lekcjach biologii się nie uważało ;)
            Czy pielęgniarka w szkole może pytać dzieciaki czy są poddane eksperymentalnej szczepionce na c19?
            Ale to Pani partia pokłóciła nas nawet z Węgrami więc po co te puste gadanie
            A Pan nadal wierzy że jesteśmy krajem niepodległym?
            Trzeba było nie wędrować po knajpach ;)
            Przez ostatnie 2 lata to obecny rząd zniewalał Polaków i uśmiercił 200tys ludzi także lepiej przemyśleć to co się pisze!!!
            Kłamiesz, nie odczuwamy, a wręcz przeciwnie!!!
            Słusznie w polszmacie i rządzie same małpy. Zabezpieczanie się :)
            Znajomy zapaleniec ma kilkanascie sztuk broni i z tego co wiem do tej pory nikogo nie zabił.
            Ty jesteś zwyczajnie jebn...ty??? Kiedyś uważałem cię za porządnego dziennikarza ale widać że jesteś sprzedajna ku...
            Jakie rozkazy dostaliście do wykonania?
            Kolega co pracuje w cocacoli powiedział kilka dni temu że obi teraz biją rekordy sprzedaży także widać Polakom drożyzna nie przeszkadza…
            A sprzedaż "małpek"?
            No niezłych tam ekspertów macie ;)
            Czy najgorszy rząd w dziejach mógłby już skończyć swoją przestępczą działalność?
            To jest objaw bezobjawowej małpiej ospy. Zalecam konsultacje z
            A kto wam kazał jak małpy się ubierać i wspierać ten pandemiczny cyrk?
            W jaki sposób zbadano że wodór roztapia się na hel? Wodór jest pierwszym pierwiastkiem w układzie mendelejewa czy skoro się „roztapia” to nie powinno to jakoś inaczej być?
            Ale ja nie mam norweskich znajomych :(
            Źle! Najpierw był Mirawietzky ;)
            I po doradcy Tuska mordowieckim...
            Kolejny comming out czerwonego
            Dziennikarze wogóle nie powinni tracić czasu na mówienie o tych kolejnych bzdurach żeby nie napędzać przemysłu strachu!
            Nie sądzę żeby to gdzie wkładasz swój kij kogoś interesowało ;)
            Ot odezwał się człek godny zaufania...
            Kurde nawet taki greenpeace nic sobie z was nie robi… po co nam taki „rząd”…
            Pisowcy to idioci więc tam nie ma dla nich miejsca
            Oczywiście że nie. Że też ludzie mają takie pomysły...
            Polacy zresztą też
        """ 

        
