---
# This section is dedicated to define SharingHub model STAC Properties 

## example of stac properties (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md)
#extent:
#  spatial: "POLYGON ((-0.6012670907896336 44.99287995641083, -0.7549322239223102 45.011185759845944, -0.8660053908054977 44.87345374753511, -0.7682944094115385 44.75555835253826, -0.64719960341057 44.6813802101253, -0.3983288986645448 44.77334695637518, -0.3883072595467922 44.92551153626323, -0.6012670907896336 44.99287995641083))"
#  spatial: [-66.5902, 17.9823, -66.6407, 18.0299]
---

## Getting started

### Get Started with DVC

#### Prerequisites

As a prerequisite for using DVC, you must have a Git repository initialized :

```bash
git clone https://gitlab.develop.eoepca.org/sharinghub-test/sen1floods11-dataset.git
```

#### Authenticate DVC

Configure your authentication (will be only stored locally)

```bash
dvc remote modify --local workspace access_key_id 'mysecret'
dvc remote modify --local workspace secret_access_key 'mysecret'
```

#### Tracking data

Working inside an initialized project directory, let's pick a piece of data to work with. We'll use an example `very_big_file.txt` file, in the `data` directory.

```bash
echo "very big content" > data/very_big_file.txt
```

Use `dvc add` to start tracking the dataset file:

```bash
dvc add data/very_big_file.txt
```

DVC stores information about the added file in a special `.dvc` file named `data/very_big_file.txt.dvc`. This small, human-readable metadata file acts as a placeholder for the original data for the purpose of Git tracking.

Next, run the following commands to track changes in Git:

```bash
git add data
git commit -m "chore: add raw data"
dvc push
git git push --set-upstream origin main
```
