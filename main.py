import os
import collections
from pymongo import MongoClient
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from bson.objectid import ObjectId
import yaml

# Connect to the MongoDB database
db_client = MongoClient(os.environ["MONGO_URI"])
db = db_client[os.environ["DB_NAME"]]
collection = db[os.environ["COLLECTION_NAME"]]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/assign/{condition}")
async def assign_to_chain(condition: int):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    chain = collection.find_one(
        {"condition": condition, "busy": False}, sort=[("writes", 1)]
    )
    if chain is None:
        return 404

    chain["busy"] = True
    collection.update_one({"_id": chain["_id"]}, {"$set": chain})

    chain["_id"] = str(chain["_id"])
    return chain


@app.get("/assign/no-busy/{condition}")
async def assign_to_chain_no_busy(condition: int):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Then return that chain without marking it busy.
    """
    chain = collection.find_one(
        {"condition": condition, "busy": False}, sort=[("reads", 1)]
    )
    if chain is None:
        return 404

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


@app.post("/free/{chain_id}")
async def free_chain(chain_id: str):
    """
    Set the specified chain to no be busy anymore (without adding any data).
    """
    chain_id = ObjectId(chain_id)
    chain = collection.find_one({"_id": chain_id})
    if chain is None:
        return 404

    chain["reads"] += 1
    chain["busy"] = False
    res = collection.update_one({"_id": chain_id}, {"$set": chain})

    return res.raw_result


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
    chain["writes"] += 1
    chain["reads"] += 1
    res = collection.update_one({"_id": chain_id}, {"$set": chain})

    return res.raw_result


@app.post("/chain/read/{chain_id}")
async def update_chain_read_count(chain_id: str):
    """
    Add a message to the chain with the given chain_id, then mark the chain as no longer in use.
    """
    chain_id = ObjectId(chain_id)
    chain = collection.find_one({"_id": chain_id})

    chain["reads"] += 1
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
    conditions: list
    n_chains_per_condition: int


@app.post("/setup")
async def set_up_chains(body: SetupBody):
    """
    Create all the chains.
    """
    to_insert = []
    for condition in body.conditions:
        for _ in range(body.n_chains_per_condition):
            to_insert.append(
                {
                    "condition": condition,
                    "messages": [],
                    "busy": False,
                    "writes": 0,
                    "reads": 0,
                }
            )

    res = collection.insert_many(to_insert)

    return {
        "inserted_ids": [str(oid) for oid in res.inserted_ids],
        "acknowledged": res.acknowledged,
    }
