from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import remove_RT, drop_na_text, join_user_text

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [ 
            node(
                func=drop_na_text,
                inputs="twitts",
                outputs="twitts_no_na",
                name="remove_rows_with_no_text_node",
            ),
            node(
                func=remove_RT,
                inputs="twitts_no_na",
                outputs="twitts_removed_RT",
                name="remove_rt_node",
            ),
            node(
                func=join_user_text,
                inputs="remove_rt_node",
                outputs="concatianted_timeline",
                name="concatinate_user_timeline",
            )

        ],
        namespace="data_processing",
        inputs=["twitts"],
        outputs="twitts_removed_RT",

    )
    
