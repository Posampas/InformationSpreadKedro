import unittest
import pandas as pd

from src.informationspread.processors.xml_parser import XmlParser


class XmlParserTest(unittest.TestCase):

    def setUp(self) -> None:
        self.xml_text = ['<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE chunkList SYSTEM "ccl.dtd">\n<chunkList>\n <chunk id="ch1" type="p">\n  <sentence id="s1">\n   <tok>\n    <orth>To</orth>\n    <lex disamb="1"><base>to</base><ctag>pred</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <tok>\n    <orth>jest</orth>\n    <lex disamb="1"><base>być</base><ctag>fin:sg:ter:imperf</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <tok>\n    <orth>Warszawa</orth>\n    <lex disamb="1"><base>Warszawa</base><ctag>subst:sg:nom:f</ctag></lex>\n    <lex disamb="1"><base>warszawa</base><ctag>subst:sg:nom:f</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">1</ann>\n   </tok>\n   <tok>\n    <orth>i</orth>\n    <lex disamb="1"><base>i</base><ctag>conj</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <tok>\n    <orth>Kraków</orth>\n    <lex disamb="1"><base>Kraków</base><ctag>subst:sg:nom:m3</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">2</ann>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>.</orth>\n    <lex disamb="1"><base>.</base><ctag>interp</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n  </sentence>\n </chunk>\n</chunkList>\n', '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE chunkList SYSTEM "ccl.dtd">\n<chunkList>\n <chunk id="ch1" type="p">\n  <sentence id="s1">\n   <tok>\n    <orth>A</orth>\n    <lex disamb="1"><base>a</base><ctag>conj</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n    <prop key="polarity:20135">0</prop>\n   </tok>\n   <tok>\n    <orth>to</orth>\n    <lex disamb="1"><base>to</base><ctag>pred</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <tok>\n    <orth>jest</orth>\n    <lex disamb="1"><base>być</base><ctag>fin:sg:ter:imperf</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <tok>\n    <orth>Brześć</orth>\n    <lex disamb="1"><base>Brześć</base><ctag>subst:sg:nom:m3</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">1</ann>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>.</orth>\n    <lex disamb="1"><base>.</base><ctag>interp</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n  </sentence>\n </chunk>\n</chunkList>\n']
        self.xml_geo_names = ['<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE chunkList SYSTEM "ccl.dtd">\n<chunkList>\n <chunk type="p" id="ch1">\n  <sentence id="s1">\n   <tok>\n    <orth>Warszawa</orth>\n    <lex disamb="1"><base>Warszawa</base><ctag>subst:sg:nom:f</ctag></lex>\n    <lex disamb="1"><base>warszawa</base><ctag>subst:sg:nom:f</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">1</ann>\n    <prop key="nam_loc_gpe_city:coord:1">21.0714111288323;52.2337172;administrative;Warszawa, województwo mazowieckie, Polska</prop>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>;</orth>\n    <lex disamb="1"><base>;</base><ctag>interp</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>Kraków</orth>\n    <lex disamb="1"><base>Kraków</base><ctag>subst:sg:nom:m3</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">3</ann>\n    <prop key="nam_loc_gpe_city:coord:1">19.9970637622827;50.04691015;administrative;Kraków, województwo małopolskie, Polska</prop>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>;</orth>\n    <lex disamb="1"><base>;</base><ctag>interp</ctag></lex>\n    <ann chan="nam_loc_gpe_city">0</ann>\n   </tok>\n   <ns/>\n   <tok>\n    <orth>Brześć</orth>\n    <lex disamb="1"><base>Brześć</base><ctag>subst:sg:nom:m3</ctag></lex>\n    <ann chan="nam_loc_gpe_city" head="1">2</ann>\n    <prop key="nam_loc_gpe_city:coord:1">23.6851851;52.093751;city;Brześć, obwód brzeski, Białoruś</prop>\n    <prop key="nam_loc_gpe_city:coord:2">19.0355556;52.6372222;locality;Brześć, Włocławek, województwo kujawsko-pomorskie, 87-800, Polska</prop>\n    <prop key="nam_loc_gpe_city:coord:3">18.4234903;52.559434;village;Brześć, gmina Kruszwica, powiat inowrocławski, województwo kujawsko-pomorskie, 88-121, Polska</prop>\n   </tok>\n  </sentence>\n </chunk>\n</chunkList>']
   
    def test_should_accept_xml_string_as_constructor(self):
        xml_text = "xml_string"
        parser = XmlParser(xml_text)
        self.assertEqual(xml_text, parser.xml_string)

    def test_should_extrat_base_version_of_word_that_have_geo_annotation(self):
        xml_text = self.xml_text[0]
        parser = XmlParser(xml_text)
        result = parser.extractBaseFormOfWordsThatHasGeoAnnotation()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertEqual("Warszawa;Kraków", result)

    def test_should_extract_geo_information(self):
        xml_text = self.xml_geo_names[0]
        parser = XmlParser(xml_text)
        result = parser.extract_geo_addnotations()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual(result.columns.to_list(), ['country', 'city', 'lat', 'lon'])
        print(result)
        self.assertEqual(len(result), 5)
        self.assertListEqual(result.iloc[0].tolist(), ["Polska","Warszawa", 52.2337172,21.0714111288323 ])
        self.assertListEqual(result.iloc[1].tolist(), ["Polska","Kraków", 50.04691015,19.9970637622827 ])
        self.assertListEqual(result.iloc[2].tolist(), ["Białoruś","Brześć", 52.093751,23.6851851 ])
