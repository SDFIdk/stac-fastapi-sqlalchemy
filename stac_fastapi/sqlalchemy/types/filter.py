from typing import List, Tuple
from stac_fastapi.sqlalchemy.extensions.filter import BaseQueryables, SkraafotosProperties

class Queryables:
    # TODO: Get collections from database
    base_queryables = [q.value for q in BaseQueryables]
    collections = {
        "skraafotos2017": SkraafotosProperties,
        "skraafotos2019": SkraafotosProperties,
        "skraafotos2021": SkraafotosProperties,
        "skraafotos2023": SkraafotosProperties,
    }

    @classmethod
    def get_queryable(cls, name):
        if name in BaseQueryables._value2member_map_:
            return BaseQueryables(name)
        for c in cls.collections.values():
            if name in c._value2member_map_:
                return c(name)

    @classmethod
    def get_queryable_properties_intersection(
        cls, collection_ids: List = []
    ) -> Tuple[List, List]:
        if len(collection_ids) == 0:
            collection_ids = (
                cls.collections.keys()
            )  # empty defaults to intersection across all collections
        all_queryables = []
        for collection in collection_ids:
            if collection in cls.collections:
                all_queryables.append(
                    [
                        q.value
                        for q in cls.collections[collection]
                        if q not in cls.base_queryables
                    ]
                )

        shared_queryables = (
            set.intersection(*[set(x) for x in all_queryables])
            if len(all_queryables) > 0
            else set()
        )
        return cls.base_queryables, list(shared_queryables)

    @classmethod
    def get_all_queryables(cls) -> List[str]:
        all_queryables = cls.base_queryables
        for collection in cls.collections.values():
            all_queryables = all_queryables + [q.value for q in collection]
        return list(set(all_queryables))
