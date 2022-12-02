db = new Mongo().getDB("test");

db.createCollection("test_collection", { capped: false });


db.test_read_collection.insert({ name: "name_1", value: 1 })
db.test_read_collection.insert({ name: "name_2", value: 2 })
db.test_read_collection.insert({ name: "name_3", value: 3 })
db.test_read_collection.insert({ name: "name_4", value: 4 })
db.test_read_collection.insert({ name: "name_5", value: 5 })
db.test_read_collection.insert({ name: "name_6", value: 6 })
db.test_read_collection.insert({ name: "name_7", value: 7 })
db.test_read_collection.insert({ name: "name_8", value: 8 })
db.test_read_collection.insert({ name: "name_9", value: 9 })
db.test_read_collection.insert({ name: "name_10", value: 10 })