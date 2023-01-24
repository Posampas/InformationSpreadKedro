import pandas as pd
from ...processors.clarinService import ClarinService 
from ...processors.xml_parser import XmlParser


def extract_words_with_geo_assosiation_and_convert_it_to_base_form(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Returns base form of the words that have geo annotation.
    Args : dataframe containg columns text and id

    Retruns:
        twitts: where in text are only presents the words that have geo annotation
    """
    _throw_if_column_not_present('text', twitts)
    _throw_if_column_not_present('user_id', twitts)
    lmpn = 'any2txt|wcrft2({"morfeusz2":false})|liner2({"model":"n82"})|ccl_emo({"lang":"polish"})'
    total_len = len(twitts)
    for i , row  in twitts.iterrows():
        print(i + 1 , "/", total_len, 'user_id',row['user_id'])
        text = row['text'][:20000]
        baseFromService = ClarinService(text, lmpn)
        response = baseFromService.run()
        parsers = list(map(lambda x: XmlParser(x), response))
        transformed = list(map(lambda x: x.extractBaseFormOfWordsThatHasGeoAnnotation(), parsers))
        transformed = filter(lambda x : x, transformed) # remove resposenes that are empty
        joined = ';'.join(transformed)
        twitts.at[i , 'text'] = joined

    return twitts

def transform_place_names_to_geo_cordinates(twitts: pd.DataFrame) -> pd.DataFrame:
    """  
    Args : dataframe containg columns text and id, where in text it expects to have been
        provided with list string of Places containg names of places separated by semicolon ';'

    Retruns:
        twitts: 
    """
    _throw_if_column_not_present('user_id',twitts)
    _throw_if_column_not_present('text',twitts)
    users_geo_location_frames = []
    for i, row in twitts.iterrows():
        print(row['text'])
        text = row['text'][:20000]
        current_user_frame  = _get_cordinates_for_text(text)
        current_user_frame['user_id'] = row['user_id']
        users_geo_location_frames.append(current_user_frame)
        
    return pd.concat(users_geo_location_frames)

def _get_cordinates_for_text(text : str) :
    lmpn = 'any2txt|wcrft2({"morfeusz2":false})|liner2({"model":"n82"})|geolocation({"limit":1})'
    service = ClarinService(text=text,lmpn = lmpn)
    response = service.run()
    parsers = list(map(lambda x: XmlParser(x), response))
    transformed = list(map(lambda x: x.extract_geo_addnotations(), parsers))
    
    return pd.concat(transformed)



def _throw_if_column_not_present(column:str, twitts: pd.DataFrame) -> None:
    if (column not in twitts.columns.to_list()):
        raise RuntimeError("Column \"{}\" has to be present in the input frame".format(column)) 