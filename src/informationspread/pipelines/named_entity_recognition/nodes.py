import pandas as pd
import stanza


def recognise_named_entites_stanza(twitts: pd.DataFrame) -> pd.DataFrame:
    """  
    Args : Dataframe containg colums user_id and text

    It extract from text entites that can be recognised.

    Retruns:
        dataFrame containg columns user_id, entity and type 
    """ 
    entity_col = 'entity'
    entity_type_col = 'ent_type'
    id_col = 'user_id'
    stanza.download('pl')
    nlp = stanza.Pipeline(lang='pl', processors='tokenize,mwt,pos,ner')
    result_frame = pd.DataFrame()
    for id, row in twitts.iterrows():
        doc = nlp(row['text'])
        entity = []
        types  = [] 
        for ent in doc.ents:
            entity.append(ent.text)
            types.append(ent.type) 
    row_frame = pd.DataFrame({entity_col : entity , entity_type_col : types})
    row_frame[id_col] = row[id_col]
    result_frame = pd.concat([result_frame, row_frame], axis=0, ignore_index=True)
        
    return result_frame
    