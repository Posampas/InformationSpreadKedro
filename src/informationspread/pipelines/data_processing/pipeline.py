from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from nodes import remove_RT

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=remove_RT,
                inputs="twitts",
                outputs="twitts_removed_RT",
                name="remove_rt_node",
            ),
            
        ],
        namespace="data_processing",
        inputs=["twitts"],
        outputs="processed",
    )
    
