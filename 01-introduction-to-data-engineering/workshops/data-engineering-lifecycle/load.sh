#!/bin/bash

API_KEY='$2b$10$cVFpEJAKuFa7keZdnLg3auRWw/.s2C7gQnkqKVNf4ga.7W85y6rt.'
COLLECTION_ID='64cdf955b89b1e2299cbadff'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"
