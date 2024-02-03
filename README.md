# core-geospatial-service
A platform for gathering, cleaning and presenting geospatial data in a easy to 
use API. The service allows you to index satellite imagery in the AWS sentinel 2
S3 bucket and then query data spatially. Users can create accounts, manage
boundaries saved in their account and view maps built from satellite data. Currently
the service can only build NDVI maps, but you can add other map types as needed.

The repository is broken up into two components. One is the worker which performs
tasks like creating the spatial index and building maps, and the other is the 
api which allows users to interact with the service. If you just want to create 
a spatial index then you only need to run the worker.


## Satellite imagery
The service will pull data from the public AWS Sentinel 2 S3 database and perform
any cleaning or transformations as needed.

Link to AWS info: https://registry.opendata.aws/sentinel-2-l2a-cogs/


## Set Environment Variables
In the `core_service/environment.env` file you'll need to set the NDVI scripts' path so the Golang
program can run the python program among other environment specific informations. Also
setup other variables so they match your environment.

You will need to at least set these two
```
NDVI_SCRIPT="/repo-path-here/core_service/pyGeoSpatialApp/build_ndvi_map.py"
PROJECT_PYTHON_PATH="/repo-path-here/.venv/bin/python"
```

You can also set the UI build path for a react build directory. A UI is not included with this repository.


## Setup the Python Virtual Environment
Run the following from the base directory of this repository
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r core_service/pyGeoSpatialApp/requirements.txt
```

## Run the App
To run the app you can do so from the `/core_service` directory of the project
using these commands:
```
docker compose up mongo s3mock
go run core_service api
go run core_service worker
```
The first go command boots up the API and the second go command boots up a worker
which will start processing events stored in the Mongodb database.


## Load Data into Database
Create the database
```
use geo_spatial
```

First we need to set some bounds on the amount of data the service will sync. In the 
Mongodb shell execute the following statement
```
db.setting.insertOne({
    utm_zones: [ '15T' ],
    tile_files: [ 'B04.tif', 'B08.tif', 'SCL.tif' ],
    tile_start_date: ISODate("2023-08-01T00:00:00.000Z")
})
```
In this case I'm restricting my system to only index and allow for map generation
in the 15T utm zone with band 4 and band 8 data after the first of august of 2023.

Add the following event to the database to sync the current satellite image
file listing into the database
```
db.event.insertOne({event_type: "RequestCurrentIndexFilesTask", max_attempts: 1, priority: 5, started: false, failed: false, passed: false, attempts: 0, start_after_date: new ISODate("1970-01-01T00:00:00.000Z")})
```

Now boot up the worker and wait for the files to sync in the database. When this
process is done you should have no events left to process in the `event` collection.
There might be some that failed but this is expected.
```
go run core_service worker
```

You should see that the worker is downloading csv files from the s3 inventory bucket. This
process will take some time to complete.

To clear out all data and reset the systems state you can run the following commands
```
db.event.deleteMany({})
db.tile.deleteMany({})
```

Retry manifest load event with a specific manifest date
```
db.event.updateOne({event_type: "RequestCurrentIndexFilesTask"}, {$set: {priority: 10, started: false, failed: false, errors: null, attempts: 0, data: {manifestDate: '2023-09-05'}}})
```

Retry a build boundary map task
```
db.event.updateOne({event_type: 'BuildBoundaryMapTask', data: {mgrsCode: '15TUL'}}, {$set: {priority: 10, started: false, failed: false, errors: null, attempts: 0}})
```

## Run the Local Cache
To run the local cache you will need to boot up the Nginx container and point the service
at that container. In the `environment.env` file update the satellite image variable to this
```
SATELLITE_S3_IMAGE_ENDPOINT="http://localhost:5005"
```
Now data will be cached by Nginx up to 10 gb. Only use this if you plan on requesting
a few satellite image files frequently. 


## Run Python Tests
Run this command from the `core_service/pyGeoSpatialApp/` directory of this project:
```
python -m unittest test_build_ndvi_map.py
```
*These tests are currently out dated.


## Run all Go Tests
Note: the tests use the `test_db` in the Mongodb they run against. This database is deleted
and recreated after each test so make sure you are pointed at the Mongodb database in this
repository.

The s3mock and mongo containers must be running for these tests.

The following code runs all tests sequentially without caching. The tests use the database
and need to clear it out at the start of each run, so they must be run one at a time.
```
go test -count=1 -p=1 core_service/...
```


## MongoDb Access

The mongo docker containers default password is `pass`

```
mongosh "mongodb://root@localhost:27017/?authSource=admin"
```

## Library Notice

In this code base I needed to use code from `https://github.com/tzneal/coordconv/tree/master` in 
the package `geoTransformations` for converting between coordinate systems. This package is 
almost identical with small small modifications. I copied it over just in case the repository
is ever removed from Github. The repository has a public domain license and can be used for 
any purpose so redistribution here is okay.
