from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import remove_RT, drop_na_text, join_user_text, remove_non_polish_tweets, remove_regex_from_text, remove_non_ascii_chars


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
                func=remove_non_polish_tweets,
                inputs="twitts_removed_RT",
                outputs="twitts_only_in_polish",
                name="remove_non_polish_twitts_node",
            ),
            node(
                func=remove_regex_from_text,
                inputs=["twitts_only_in_polish","params:emojji_regex"],
                outputs="twitts_asci_chars",
                name="remove_not_asci_chars_node",
            ),
            node(
                func=remove_regex_from_text,
                inputs=["twitts_asci_chars", 'params:user_mention_reqex'],
                outputs="twitts_with_mentions_removed",
                name="remove_mentions_from_text_node",
            ),
            node(
                func=remove_regex_from_text,
                inputs=["twitts_with_mentions_removed", 'params:twitter_links_reex'],
                outputs="twitts_with_links_removed",
                name="remove_links_from_text_node",
            ),
            node(
                func=join_user_text,
                inputs="twitts_with_links_removed",
                outputs="cleaned_data",
                name="concatinate_user_timeline",
            )
        ],
        namespace="data_processing",
        inputs=["twitts"],
        outputs="cleaned_data",
    )

