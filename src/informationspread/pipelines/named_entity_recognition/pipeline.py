"""
This is a boilerplate pipeline 'named_entity_recognition'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import recognise_named_entites_stanza

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=recognise_named_entites_stanza,
                inputs="cleaned_data",
                outputs="stanza_ner",
                name="recognise_named_entites_with_stanza",
            ),
        ]
        ,
        namespace="ner_stanza",
        inputs="cleaned_data",
        outputs="stanza_ner",
    )
