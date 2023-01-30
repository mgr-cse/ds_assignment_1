# ds_assignment_1

Distributed Queue assignment

## Code structure
* `broker_part_a.py` contains the in-memory implemetation of the broker for Part A
* `broker_part_b.py` contains broker implementation with presistant storage and supports recovery from a crash.
* `queueSDK` module contains the library implementation for Part C
* `tests` directory contains testcases to test both `broker_part_a.py` and `broker_part_b.py`
* `test_asgn1` contains test cases supplied in the assignment statement.
* `{consumer|producer}_process.py` runs the testcase using Sdk

## Prerequisites
The assignment is done using:
* `flask` as http request handler
* `SQLAlchemy` as ORM
* `Python 3.10.6`
* `postgres` as database

## Installing prerequisites

Install the required python system packages: 
```bash
sudo apt install python3-venv python3-pip postgresql
```
### Setting up repository
```bash
git clone https://github.com/mgr-cse/ds_assignment_1
cd ds_assignment_1
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```
### Setting up database
```bash
sudo systemctl start postgresql
sudo -iu postgres psql < create_database.sql
```
## Running tests
* For part A
```bash
# run the broker
python broker_part_a.py
# in a seperate window run
./tests/part_a.py
```
* similarly for Part B
```bash
# run the broker
python broker_part_b.py
# in a seperate window run
./tests/part_b.py
```
You may want to crash the broker for part B by sending `SIGINT` (`Ctrl-C`), and run it again.


    