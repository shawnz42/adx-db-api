
# What's this?
- This is a [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/13/dialects/index.html) for [ADX](https://docs.microsoft.com/en-us/azure/data-explorer/) (Azure Data Explorer), which wraps azure-kusto-data api to sqlalchemy interface.

- Another enhancement is supporting transformation from sql to [kql](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/)(Kusto query language), for example , it can transform 

```shell script
select * 
from storm
where col = 'a' 
limit 10
```
 to
```shell script
storm 
| where col == 'a'
| limit 10
```
- Helpful for data scientist, see integrate with pandas

Enjoy it!

# How to install this package?

- first prepare your python environment and install necessary packages
```shell script
mkdir adx-db-api-demo
cd adx-db-api-demo
virtualenv venv -p `which python3`
source venv/bin/activate
pip install sqlalchemy
pip install azure-kusto-data==2.3.1
```

- then install adx-db-api:
```shell script
cd adx-db-api
python setup.py install
```

- if you want to clean the asset of install:
```shell script
cd adx-db-api
python setup.py clean
```

# How to use it?

- first you may read [this page](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) to create client_id, client_secret and authority_id for your adx and get knowledge of your cluster url, then you can get data using sqlachemy interface.
- create engine:
```
from sqlalchemy import create_engine

cluster = "<insert here your cluster name>"
client_id = "<insert here your AAD application id>"
client_secret = "<insert here your AAD application key>"
authority_id = "<insert here your AAD tenant id>"

engine = create_engine('adx://client_id:client_secret@cluster/authority_id/database')
```

- support kql(Kusto  query language) 
```
result = engine.execute("table |  limit 3")

for row in result:
    print(row)
```

- also support sql 
```
result = engine.execute("select * from table limit 3")

for row in result:
    print(row)
```
you may view [SQL to Kusto cheat sheet](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet) or  the test cases.

# How to integrate with pandas?
```shell script
import pandas as pd
sql = """table |  limit 3"""  
# sql = """select * from table limit 3"""  # both kql or sql are ok

df = pd.read_sql(sql, con=engine)

print(df.head())
```

# How to test it?
```shell script
cd adx-db-api
virtualenv venv -p `which python3`
source venv/bin/activate
pip install -r requirements.txt
```

then execute the test cases.
