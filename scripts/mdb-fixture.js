db.null_test.insertMany([{a:1},{a:null}]);
db.insert_test.drop();
db.insert_test.insertOne({"a":0,"b":0,"c":0});
db.insert_test.find();
