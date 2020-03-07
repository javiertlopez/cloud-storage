import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import keyvalue.dynamostorage as DynamoStorage

BATCH_LIMIT = 24
IMAGE_LIMIT = 2000
IMAGES_FILE = "images.ttl"
LABELS_FILE = "labels_en.ttl"
AWS_REGION = "us-east-1"
IMAGE_TYPE = "http://xmlns.com/foaf/0.1/depiction"
LABEL_TYPE = "http://www.w3.org/2000/01/rdf-schema#label"

# Init parsers
image_parser = ParseTripe.ParseTriples(IMAGES_FILE)
label_parser = ParseTripe.ParseTriples(LABELS_FILE)

# Make connections to Dynamodb
db_images = DynamoStorage.DynamoStorage('images', region=AWS_REGION)
db_labels = DynamoStorage.DynamoStorage('terms', True, AWS_REGION)

# Internal storage
allImages = {}
allLabels = {}
images = {}
labels = {}

# Process images
i = 0
for x in range(IMAGE_LIMIT):
    image = image_parser.getNext()

    if image[1] == IMAGE_TYPE:
        images[image[0]] = [image[2]]
        allImages[image[0]] = image[2]

        if i % BATCH_LIMIT == 0:
            db_images.putAll(images)
            images = {}

        i+=1

db_images.putAll(images)

# Process labels
label = label_parser.getNext()
i = 0
while(label):
    if label[1] == LABEL_TYPE:
        if label[0] in allImages:
            split = label[2].split()
            if len(split) > 1:
                for item in split:
                    if Stemmer.stem(item) in allLabels:
                        allLabels[Stemmer.stem(item)] += 1
                    else:
                        allLabels[Stemmer.stem(item)] = 0
                    
                    labels[Stemmer.stem(item)] = [label[0],allLabels[Stemmer.stem(item)]]

                    if i % BATCH_LIMIT == 0:
                        db_labels.putAll(labels)
                        labels = {}

                    i+=1
            else:
                if Stemmer.stem(label[2]) in allLabels:
                    allLabels[Stemmer.stem(label[2])] += 1
                else:
                    allLabels[Stemmer.stem(label[2])] = 0

                labels[Stemmer.stem(label[2])] = [label[0], allLabels[Stemmer.stem(label[2])]]

                if i % BATCH_LIMIT == 0:
                    db_labels.putAll(labels)
                    labels = {}

                i+=1

    label = label_parser.getNext()

db_labels.putAll(labels)