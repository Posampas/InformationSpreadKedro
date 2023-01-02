"""
This is a boilerplate pipeline 'data_sience'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import test_node

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [ 
            node(func=test_node,
            name="test_node",
            inputs="cleaned_data",
            outputs = None),
            
        ])
