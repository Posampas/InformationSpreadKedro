import pandas as pd
import re
from ...processors.clarinService import ClarinService 
from ...processors.xml_parser import XmlParser

def remove_RT(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Removes RT twitts from users timelines

    Args:
        users: Twitts data with no na values in tweet text
    Returns:
        users_rt_removed: 
    """
    _throw_if_column_not_present("text",twitts)
    twitts = twitts[~ twitts["text"].str.startswith('RT')]
    
    return twitts

def drop_na_text(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Removes rows that does not contains text
    Args:
        users: Twitts raw data
    Returns:
        twitts : no text na rows: 
    """
    _throw_if_column_not_present("text",twitts)
    twitts = twitts[twitts["text"].notna()]
    
    return twitts

def join_user_text(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Joins all twitts of a single user to one text
    Args:
        users: dataFrame of twitts 
    Returns:
        twitts : dataFrame where all users twitts text  are concatinated 
    """
    _throw_if_column_not_present("text",twitts)
    _throw_if_column_not_present("user_id",twitts)
    unique_users = twitts["user_id"].unique()
    text = []
    for user in unique_users:
        text.append(twitts[twitts["user_id"] == user]['text'].str.cat(sep='\n'))
        


    return  pd.DataFrame({"user_id": unique_users, "text": text})


def remove_non_polish_tweets(twitts:pd.DataFrame)->pd.DataFrame:
    """ removes twitts that has in lang column diffrent than 'pl'
    Args:
        twitts: dataFrame of twitts 
    Returns:
        twitts : dataFrame where all twitts are in polish
    """
    _throw_if_column_not_present("lang",twitts)
    return twitts[twitts["lang"] == "pl"]

def remove_regex_from_text(twitts:pd.DataFrame, regex_string : str ) -> pd.DataFrame:
    """ from twitt text removes mentions in form @username 
    Args:
        twitts: dataFrame of twitts 
    Returns:
        twitts : dataFrame where all the twitts text is cleansed from user mentions
    """
    _throw_if_column_not_present("text",twitts)

    if not regex_string or not isinstance(regex_string , str):
        raise RuntimeError("Regex must be string and be parsable to regex")
    
    regrex_pattern = re.compile(pattern = regex_string, flags = re.UNICODE)

    twitts['text'] =  twitts['text'].str.replace(regrex_pattern, '').str.strip()
    return twitts

def remove_non_ascii_chars(twitts:pd.DataFrame) -> pd.DataFrame:
    """ created to get rid of emojjis
    Args:
        twitts: dataFrame of twitts 
    Returns:
        twitts : dataFrame where all the twitts text is cleansed from user mentions
    """ 
    _throw_if_column_not_present("text",twitts)
    twitts['text'] = twitts['text'].str.encode('ascii', 'ignore').str.decode('ascii').str.strip()
    return twitts


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
    
   