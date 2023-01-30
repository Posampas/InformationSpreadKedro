## How to 
1. Install enviroment from requirents.txt
2. Train custom model
    1. Create your train data. https://tecoholic.github.io/ner-annotator/ it is a nice place to label text.
    2. Export data to file tran_data.json
    3. Place file named train_data.json in data/01_raw
    4. run command : kedro run --pipeline train_ner

3. Preprocess data for entity recognition 
    1. Place your csv file with twitts in data/01_raw and call it main.csv. This file must have at lest columns 
    user_id, lang,text
    2. run command : kedro run --pipeline data_processing

4. Run named entity recogision using:
    1. Stanza:
        1. kedro run --pipeline named_entity_recognition
    2. Clarin: (works slow)
        1. kedro run --pipeline ner_clarin
    3. Custom model
        1. kedro run --pipeline ner_custom

5. Files with recogised entites will be saved to data/07_model_output