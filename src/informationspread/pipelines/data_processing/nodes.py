import pandas as pd

def remove_RT(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Removes RT twitts from users timelines

    Args:
        users: Twitts data with no na values in tweet text
    Returns:
        users_rt_removed: 
    """
    if ("text" not in twitts.columns.to_list()):
        raise RuntimeError("Column \"text\" has to be present in the input frame")
    twitts = twitts[~ twitts["text"].str.startswith('RT')]
    
    return twitts

def drop_na_text(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Removes rows that does not contains text
    Args:
        users: Twitts raw data
    Returns:
        twitts : no text na rows: 
    """
    if ("text" not in twitts.columns.to_list()):
        raise RuntimeError("Column \"text\" has to be present in the input frame")
    twitts = twitts[twitts["text"].notna()]
    
    return twitts

def join_user_text(twitts: pd.DataFrame) -> pd.DataFrame:
    """ Joins all twitts of a single user to one text
    Args:
        users: dataFrame of twitts 
    Returns:
        twitts : dataFrame where all users twitts text  are concatinated 
    """
    if "text" not in twitts.columns.to_list() or "user_id" not in twitts.columns.to_list():
        raise RuntimeError("Column \"text\" and \"user_id\" has to be present in the input frame")
    unique_users = twitts["user_id"].unique()
    text = []
    for user in unique_users:
        text.append(twitts[twitts["user_id"] == user]['text'].str.cat(sep='\n'))
        


    return  pd.DataFrame({"user_id": unique_users, "text": text})


def remove_non_polish_tweets(twitts:pd.DataFrame)->pd.DataFrame:
    if "lang" not in twitts.columns.to_list():
        raise RuntimeError("Column \"lang\" has to be present in the input frame") 
    return twitts[twitts["lang"] == "pl"]
