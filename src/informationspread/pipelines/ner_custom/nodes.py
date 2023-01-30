"""
This is a boilerplate pipeline 'ner_custom'
generated using Kedro 0.18.3
"""
import spacy
import pandas as pd

def recoginse_named_entity_in_text_using_custom_model(input_frame:pd.DataFrame):
    entity_col = 'entity'
    entity_type_col = 'ent_type'
    id_col = 'user_id'
    nlp = spacy.load(r"./data/06_models/spacyModel/model-best")
    result_frame = pd.DataFrame()
    for id, row in input_frame.iterrows():
        entities = []
        types = []
        doc = nlp(row['text'])
        for ent in doc.ents:
            entities.append(ent.text)
            types.append(ent.label_)
        row_frame = pd.DataFrame({entity_col : entities , entity_type_col : types})
        row_frame[id_col] = row[id_col]
        result_frame = pd.concat([result_frame, row_frame], axis=0, ignore_index=True)
    
    return result_frame


    


def print_frame(input_frame: pd.DataFrame):
    print(input_frame)
