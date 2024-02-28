print("--- null_test fixture ---");
db.null_test.drop();
db.null_test.insertMany([{a:1},{a:null}]);
printjson(db.null_test.find());
print("--- insert_test fixture ---");
db.insert_test.drop();
db.insert_test.insertOne({"a":0,"b":0,"c":0});
printjson(db.insert_test.find());
print("---");
