import xml.etree.ElementTree as ET
import pandas as pd

class XmlParser():
    """
        Used for extract information form xmlFiles sent form claring sercive.
    """
    
    def __init__(self, xml_string : str) -> None:
        self.xml_string = xml_string

    def extractBaseFormOfWordsThatHasGeoAnnotation(self):
        root = ET.fromstring(self.xml_string)
        tokens = root.findall('./chunk/sentence/tok')
        base_tags = []
        for tok in tokens:
            if self._token_does_not_have_geo_annotation(tok):
                continue
            base_tags.append(tok.find('orth'))
        base_words = list(map(lambda x: x.text,base_tags))
        return ';'.join(base_words)

    def extract_geo_addnotations(self) ->pd.DataFrame:
        frame = pd.DataFrame()
        root = ET.fromstring(self.xml_string)
        tokens = root.findall('./chunk/sentence/tok/prop')
        print(list(map(lambda t : t.text, tokens)))
        geo = {"country" :[], "city" : [], "lat" : [] , "lon" : []}
        for tok in tokens:
            geo_text = tok.text.split(";")
            geo['lat'].append(float(geo_text[1]))
            geo['lon'].append(float(geo_text[0]))
            names = geo_text[3].split(",")
            geo['country'].append(names[len(names) - 1 ].strip())
            geo['city'].append(names[0].strip())
        return pd.DataFrame(geo)
    
    def _token_does_not_have_geo_annotation(self,token):
        annotations = token.findall('./ann')
        for ann in annotations:
            if ann.attrib['chan'] == 'nam_loc_gpe_city' and int(ann.text) > 0:
                return False
        return True

if __name__ == "__main__":
    old = pd.read_csv('cites.csv')
    print(old)
    response_string = None
    with open('response.xml') as file:
        response_string =  "".join(file.readlines())
    parser = XmlParser(response_string)
    frame = parser.extract_geo_addnotations()
    frame.drop_duplicates(inplace=True)
    frame = pd.concat([old,frame])
    frame.to_csv('cites.csv', columns=frame.columns.to_list(), index= False)
