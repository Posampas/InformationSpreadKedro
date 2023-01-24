import pandas as pd
import re


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



def _throw_if_column_not_present(column:str, twitts: pd.DataFrame) -> None:
    if (column not in twitts.columns.to_list()):
        raise RuntimeError("Column \"{}\" has to be present in the input frame".format(column)) 
    
   