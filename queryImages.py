import keyvalue.stemmer as Stemmer
import keyvalue.dynamostorage as DynamoStorage

import sys

AWS_REGION = "us-east-1"

# Make connections to Dynamodb
db_images = DynamoStorage.DynamoStorage('images', region=AWS_REGION)
db_labels = DynamoStorage.DynamoStorage('terms', True, AWS_REGION)

search = []

if len(sys.argv) > 1:
    for x in range(1, len(sys.argv)):
        search.append(Stemmer.stem(sys.argv[x]))

result = db_labels.getAll(search)
images = db_images.getAll(result)

for image in images:
    print(image)
