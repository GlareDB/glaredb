import bson

with open("/Users/corygrinstead/Development/glaredb/copy_output.bson", "rb") as f:
    for idx, doc in enumerate(bson.decode_file_iter(f)):
        print(idx, doc)
        assert len(doc) == 1
        assert "amount" in doc