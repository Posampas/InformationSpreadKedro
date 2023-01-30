"""
This is a boilerplate pipeline 'ner_clarin'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import extract_words_with_geo_assosiation_and_convert_it_to_base_form

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
    [
        node(
            func=extract_words_with_geo_assosiation_and_convert_it_to_base_form,
            inputs="cleaned_data",
            outputs="clarin_ner",
            name = "recogise_entites_with_geo_assosiation"
            )
    ],
    namespace = "ner_clarin",
    inputs = "cleaned_data",
    outputs = "clarin_ner"
    )
