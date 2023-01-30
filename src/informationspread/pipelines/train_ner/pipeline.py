"""
This is a boilerplate pipeline 'train_ner'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import transform_data_to_spacy_format, train_spacey_model

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=transform_data_to_spacy_format,
                inputs="train_data",
                outputs="train_data_path",
                name="transform_json_to_spacy_format_node",
            ),
            node(
                func=train_spacey_model,
                inputs=["train_data_path","train_data_path"],
                outputs=None,
                name="train_spacy_model_node",
            ),
        ],
        inputs=["train_data"],
        outputs=None
    )
