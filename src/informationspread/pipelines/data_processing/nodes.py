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
