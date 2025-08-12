<img width="200" height="200" alt="image" src="https://github.com/user-attachments/assets/a0a844c8-f8b2-40ae-8d6a-78722574cacf" />


# DuckDB HTTP Server Extension
This extension transforms **DuckDB** instances into tiny multi-player **HTTP OLAP API** services.<br>
Supports Authentication _(Basic Auth or X-Token)_ and includes the _play_ SQL user interface.

> This DuckDB extension was created by [Query.Farm](https://query.farm/), where we develop and maintain many extensions that expand DuckDB’s capabilities by connecting it to new data sources, formats, and features. You can explore our full collection at [Query.Farm](https://query.farm/)

### Features

- Turn any [DuckDB](https://duckdb.org) instance into an **HTTP OLAP API** Server
- Use the embedded **Play User Interface** to query and visualize data 
- Pair with [chsql](https://community-extensions.duckdb.org/extensions/chsql.html) extension for **ClickHouse flavoured SQL**
- Work with local and remote datasets including [MotherDuck](https://motherduck.com) 🐤
- _100% Opensource, ready to use and extend by the Community!_

<br>

![image](https://github.com/user-attachments/assets/e930a8d2-b3e4-454e-ba12-e5e91b30bfbe)

#### Extension Functions
- `httpserve_start(host, port, auth)`: starts the server using provided parameters
- `httpserve_stop()`: stops the server thread

#### Notes

🛑 Run DuckDB in `-readonly` mode for enhanced security

<br>

### 📦 [Installation](https://community-extensions.duckdb.org/extensions/httpserver.html)
```sql
INSTALL httpserver FROM community;
LOAD httpserver;
```

### 🔌 Usage
Start the HTTP server providing the `host`, `port` and `auth` parameters.<br>
> * If you want no authentication, just pass an empty string as parameter.<br>
> * If you want the API run in foreground set `DUCKDB_HTTPSERVER_FOREGROUND=1`
> * If you want logs set `DUCKDB_HTTPSERVER_DEBUG` or `DUCKDB_HTTPSERVER_SYSLOG`

#### Basic Auth
```sql
D SELECT httpserve_start('localhost', 9999, 'user:pass');

┌───────────────────────────────────────────────┐
│ httpserve_start('0.0.0.0', 9999, 'user:pass') │
│                    varchar                    │
├───────────────────────────────────────────────┤
│ HTTP server started on 0.0.0.0:9999           │
└───────────────────────────────────────────────┘
```
```bash
curl -X POST -d "SELECT 'hello', version()" "http://user:pass@localhost:9999/"
```

#### Token Auth
```sql
SELECT httpserve_start('localhost', 9999, 'supersecretkey');

┌───────────────────────────────────────────────┐
│ httpserve_start('0.0.0.0', 9999, 'secretkey') │
│                    varchar                    │
├───────────────────────────────────────────────┤
│ HTTP server started on 0.0.0.0:9999           │
└───────────────────────────────────────────────┘
```

Query your endpoint using the `X-API-Key` token:

```bash
curl -X POST --header "X-API-Key: secretkey" -d "SELECT 'hello', version()" "http://localhost:9999/"
```

You can perform the same action from DuckDB using HTTP `extra_http_headers`:

```sql
D CREATE SECRET extra_http_headers (
      TYPE HTTP,
      EXTRA_HTTP_HEADERS MAP{
          'X-API-Key': 'secret'
      }
  );

D SELECT * FROM duck_flock('SELECT version()', ['http://localhost:9999']);
┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.1.3      │
└─────────────┘
```



#### 👉 QUERY UI
Browse to your endpoint and use the built-in quackplay interface _(experimental)_

![image](https://github.com/user-attachments/assets/0ee751d0-7360-4d3d-949d-3fb930634ebd)

#### 👉 QUERY API
Query your API endpoint using curl GET/POST requests

```bash
curl -X POST -d "SELECT 'hello', version()" "http://localhost:9999/?default_format=JSONCompact"
```
```json
{
  "meta": [
    {
      "name": "'hello'",
      "type": "String"
    },
    {
      "name": "\"version\"()",
      "type": "String"
    }
  ],
  "data": [
    [
      "hello",
      "v1.1.3"
    ]
  ],
  "rows": 1,
  "statistics": {
    "elapsed": 0.01,
    "rows_read": 1,
    "bytes_read": 0
  }
}
```

#### 👉 CROSS-OVER EXAMPLES

You can now have DuckDB instances query each other and... _themselves!_

```sql
D LOAD json;
D LOAD httpfs;
D SELECT httpserve_start('0.0.0.0', 9999, '');
┌─────────────────────────────────────┐
│  httpserve_start('0.0.0.0', 9999)   │
│               varchar               │
├─────────────────────────────────────┤
│ HTTP server started on 0.0.0.0:9999 │
└─────────────────────────────────────┘
D SELECT * FROM read_json_auto('http://localhost:9999/?q=SELECT version()');
┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.1.3      │
└─────────────┘
```

#### Flock Macro by @carlopi
Check out this flocking macro from fellow _Italo-Amsterdammer_ @carlopi @ DuckDB Labs

![image](https://github.com/user-attachments/assets/b409ec0e-86e0-4a8d-822c-377ddbae524d)

* a DuckDB CLI, running httpserver extension
* a DuckDB from Python, running httpserver extension
* a DuckDB from the Web, querying all 3 DuckDB at the same time

<br>

<hr>

<br>

### API Documentation

#### Endpoints Overview

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/`      | GET, POST | Query API endpoint |
| `/ping`  | GET       | Health check endpoint |

#### Detailed Endpoint Specifications

##### Query API

**Methods:** `GET`, `POST`

**Parameters:**

| Parameter | Description | Supported Values |
|-----------|-------------|-------------------|
| `default_format` | Specifies the output format | `JSONEachRow`, `JSONCompact` |
| `query` | The DuckDB SQL query to execute | Any valid DuckDB SQL query |

##### Notes

- Ensure that your queries are properly formatted and escaped when sending them as part of the request.
- The root endpoint (`/`) supports both GET and POST methods, but POST is recommended for complex queries or when the query length exceeds URL length limitations.
- Always specify the `default_format` parameter to ensure consistent output formatting.

<br>

## Development

### Cloning the Repository

Clone the repository and all its submodules

```bash
git clone <your-fork-url>
git submodule update --init --recursive
```

### Setting up CLion
**Opening project:**
Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

**Debugging:**
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`. Now on a clean repository you will first need to run `make {build type}` to initialize the CMake build directory. After running make, you will be able to (re)build from CLion by using the build target we just created. If you use the CLion editor, you can create a CLion CMake profiles matching the CMake variables that are described in the makefile, and then you don't need to invoke the Makefile.

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.


### Testing

To run the E2E test install all packages necessary:

```bash
pip install -r requirements.txt
```

Then run the test suite:

```bash
pytest pytest test_http_api
```

##### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^3]
