"""
This is a boilerplate pipeline 'data_sience'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import remove_non_polish_locations
from .nodes import db_scan_node

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(func=remove_non_polish_locations,
            name="remove_location_out_side_poland",
            inputs="cleaned_data",
            outputs = "only_polish_locations"),
            node(func=db_scan_node,
            name="test_node",
            inputs="only_polish_locations",
            outputs = None),
        ])
