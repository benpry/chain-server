import collections
from pymongo import MongoClient
from fastapi import FastAPI
from pydantic import BaseModel
from bson.objectid import ObjectId
import yaml

# Load the configuration file
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Connect to the MongoDB database
db_client = MongoClient(config["mongo_uri"])
db = db_client[config["db_name"]]
collection = db[config["collection_name"]]

app = FastAPI()


@app.get("/assign/{condition}")
async def assign_to_chain(condition: int):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    chain = collection.find_one(
        {"condition": condition, "busy": False}, sort=[("completions", 1)]
    )
    if chain is None:
        return 404

    chain["busy"] = True
    collection.update_one({"_id": chain["_id"]}, {"$set": chain})

    chain["_id"] = str(chain["_id"])
    return chain


@app.get("/chain/{chain_id}")
async def get_chain(chain_id: str):
    """
    Get the chain with the given chain_id.
    """
    chain_id = ObjectId(chain_id)
    chain = collection.find_one({"_id": chain_id})
    if chain is None:
        return 404

    chain["_id"] = str(chain["_id"])
    return chain


class MessageBody(BaseModel):
    message: str


@app.post("/chain/complete/{chain_id}")
async def add_message_to_chain(chain_id: str, body: MessageBody):
    """
    Add a message to the chain with the given chain_id, then mark the chain as no longer in use.
    """
    chain_id = ObjectId(chain_id)
    chain = collection.find_one({"_id": chain_id})

    if chain is None:
        return "chain not found"

    if not chain["busy"]:
        return "chain not in use"

    message = body.message
    chain["messages"].append(message)
    chain["busy"] = False
    chain["completions"] += 1
    res = collection.update_one({"_id": chain_id}, {"$set": chain})

    return res.raw_result


@app.delete("/demolish")
async def delete_all_chains():
    """
    Delete all chains.
    """
    res = collection.delete_many({})
    return res.raw_result


class SetupBody(BaseModel):
    n_conditions: int
    n_chains_per_condition: int


@app.post("/setup")
async def set_up_chains(body: SetupBody):
    """
    Create all the chains.
    """
    to_insert = []
    for condition in range(1, body.n_conditions + 1):
        for _ in range(body.n_chains_per_condition):
            to_insert.append(
                {
                    "condition": condition,
                    "messages": [],
                    "busy": False,
                    "completions": 0,
                }
            )

    res = collection.insert_many(to_insert)

    return {
        "inserted_ids": [str(oid) for oid in res.inserted_ids],
        "acknowledged": res.acknowledged,
    }
