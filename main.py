import collections
import os

import yaml
from bson.objectid import ObjectId
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient

# Connect to the MongoDB database
db_client = MongoClient(os.environ["MONGO_URI"] if "MONGO_URI" in os.environ else "mongodb://localhost:27017/")
db = db_client[os.environ["DB_NAME"] if "DB_NAME" in os.environ else "chaindb"]
collection = db[os.environ["COLLECTION_NAME"] if "COLLECTION_NAME" in os.environ else "chains"]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def sample_chain_to_write(condition: str):
    """
    Find a chain that begins with the "condition" string, isn't busy, and has the smallest number of writes and reads.
    """
    pipeline = [
        {"$match": {"condition": {"$regex": f"^{condition}"}, "busy": False}},
        {"$group": {"_id": {"reads": "$reads", "writes": "$writes"}, "chains": {"$push": "$$ROOT"}}},
        {"$sort": {"_id.writes": 1, "_id.reads": 1}},
        {"$limit": 1},
        {"$unwind": "$chains"},
        {"$replaceRoot": {"newRoot": "$chains"}},
        {"$sample": {"size": 1}}
    ]

    chains = list(collection.aggregate(pipeline))[0]
    return chains

async def sample_chain_to_read(condition: str):
    """
    Find a chain that begins with the "condition" string, isn't busy, and has the smallest number of writes and reads.
    Plus make sure it has at least one write.
    """
    pipeline = [
        {"$match": {"condition": {"$regex": f"^{condition}"}, "busy": False, "writes": {"$gt": 0}}},
        {"$group": {"_id": {"reads": "$reads", "writes": "$writes"}, "chains": {"$push": "$$ROOT"}}},
        {"$sort": {"_id.writes": 1, "_id.reads": 1}},
        {"$limit": 1},
        {"$unwind": "$chains"},
        {"$replaceRoot": {"newRoot": "$chains"}},
        {"$sample": {"size": 1}}
    ]

    chains = list(collection.aggregate(pipeline))[0]
    return chains


@app.get("/assign/{condition}")
async def assign_to_chain(condition: str):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    chain = await sample_chain_to_write(condition)
    if chain is None:
        return 404

    chain["busy"] = True
    collection.update_one({"_id": chain["_id"]}, {"$set": chain})

    chain["_id"] = str(chain["_id"])
    return chain


@app.get("/assign/no-busy/{condition}")
async def assign_to_chain_no_busy(condition: str):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Then return that chain without marking it busy.
    """
    chain = await sample_chain_to_read(condition)
    if chain is None:
        return 404

    chain["_id"] = str(chain["_id"])
    return chain


@app.get("/chain/{chain_id}")
async def get_chain_by_id(chain_id: str):
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

    res = collection.update_one({"_id": chain_id}, {"$set": {"busy": False}})

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
