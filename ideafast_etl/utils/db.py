from airflow.providers.mongo.hooks.mongo import MongoHook


def add_mongo_value(doc: str) -> None:
    """use the Mongo Hook to add a variable to the db"""
    with MongoHook() as db:
        db.insert_one(mongo_collection="ideafast_etl", doc={"doc_id": doc})


def print_mongo_values(doc: str) -> None:
    """use the Mongo Hook to retreive a single variable from the db"""
    with MongoHook() as db:
        test_data = db.find(
            mongo_collection="ideafast_etl", query={"doc_id": doc}, find_one=True
        )
        print(test_data)


def delete_mongo_value(doc: str) -> None:
    """use the Mongo Hook to delete a variable from the db"""
    with MongoHook() as db:
        db.delete_one(mongo_collection="ideafast_etl", filter_doc={"doc_id": doc})
