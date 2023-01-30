"""
This is a boilerplate pipeline 'named_entity_recognition'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import remove_non_polish_locations, extract_words_with_geo_assosiation_and_convert_it_to_base_form, transform_place_names_to_geo_cordinates, extarct_places_with_geo_location_stanza

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extarct_places_with_geo_location_stanza,
                inputs="cleaned_data",
                outputs="locations",
                name="extract_words_with_geo_assosiations",
            ),
        ]
        ,
        namespace="ner_stanza",
        inputs="cleaned_data",
        outputs="locations",
    )
