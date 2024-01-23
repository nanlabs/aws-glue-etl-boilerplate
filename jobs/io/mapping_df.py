from .mappings import Mappings

MAPPINGS = Mappings()


def get_df_mapping(key: str) -> list[tuple]:
    return [(key, "String", key, type) for key, type in MAPPINGS[key].items()]
