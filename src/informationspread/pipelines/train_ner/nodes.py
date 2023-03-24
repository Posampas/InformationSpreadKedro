"""
This is a boilerplate pipeline 'train_ner'
generated using Kedro 0.18.3
"""
import spacy
from spacy.tokens import DocBin
import os
import pandas as pd
import subprocess

def transform_data_to_spacy_format(input_data):
    spacy_file_save_location ="./data/03_primary/train.spacy" 
    if os.path.exists(spacy_file_save_location):
        os.remove(spacy_file_save_location)

    training_data = []
    for ann in input_data["annotations"]:
        training_data.append((ann[0], ann[1]['entities'] ))

    nlp = spacy.blank("pl")

    db = DocBin()
    for text, entites in training_data:
        doc = nlp(text)
        ents = []
        for start, end, label in entites:
            span = doc.char_span(start, end, label=label)
            ents.append(span)
        doc.ents = ents
        db.add(doc)
    db.to_disk(spacy_file_save_location)
    return spacy_file_save_location


def train_spacey_model(train_data, test_data):
    spacy_config = './data/01_raw/config.cfg'
    subprocess.run(["python","-m" ,"spacy" ,"train", spacy_config, "--output",
     "./data/06_models/spacyModel" ,"--paths.train", train_data, "--paths.dev", test_data])