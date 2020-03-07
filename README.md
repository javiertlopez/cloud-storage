# cloud-storage

## Before start
Create environment
```bash
python3 -m venv venv
```

Activate environment
```bash
. venv/bin/activate
```

Install requirements

```bash
pip install -r requirements.txt
```

## Setup

**Image limit**

You can configure how many images upload.

**Default:** 2000

Modify the file `loadImages.py`, line `7`

```python
IMAGE_LIMIT = 2000
```

**File locations**

By default, datasets are `images.ttl` and `labels_en.ttl`.
You can change them in file `loadImages.py`, line `8` and line `9`

**Default:**

```python
IMAGES_FILE = "images.ttl"
LABELS_FILE = "labels_en.ttl"
```

**AWS region**

You can configure the AWS region.

**Default:** `us-east-1`

Modify the file `loadImages.py`, line `10`

Modify the file `queryImages.py`, line `6`

```python
AWS_REGION = "us-east-1"
```

## Load Images

It will try to create both tables: `terms`, `images` in the `us-east-1` region.
```bash
python3 loadImages.py
```

## Query Images
Search for images using the the terminal.
```bash
python3 queryImages.py america
```
