"""
This is a boilerplate pipeline 'ner_custom'
generated using Kedro 0.18.3
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import recoginse_named_entity_in_text_using_custom_model ,print_frame

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
    [
        node(
            func=recoginse_named_entity_in_text_using_custom_model,
            inputs="cleaned_data",
            outputs="custom_ner_result",
            name="Recoginse named entity in text using custom model"
            )
    ],
    namespace = "custom_ner_model",
    inputs = "cleaned_data",
    outputs = None
    )
