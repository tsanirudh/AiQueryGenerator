import logging
from flask import Flask, request, jsonify
import mysql.connector
from flask_cors import CORS  # Import CORS
from mysql.connector import Error
from ollama import chat
import re

app = Flask(__name__)
CORS(app)

# Allow CORS for specific origins(if needed)
# CORS(app, resources={r"/*": {"origins": "http://localhost:8000"}})

db_config = {
    'host': 'localhost',
    'user': 'user',
    'password': 'userpassword',
    'database': 'my_database'
}

return_data_num = 100
last_checked_data = None
Question = ""
return_this_data = {}


TABLE_CONTEXT = """
### MySQL Table Schema: transactions

The `transactions` table contains structured data about sales transactions. Each row represents a single transaction with detailed attributes.

#### **Table Structure:**
- `ID` (**INT, PRIMARY KEY, AUTO_INCREMENT**)  
  - Unique identifier for each transaction.
  
- `Timestamp` (**DATETIME**)  
  - The exact date and time when the transaction occurred.

- `ProductID` (**VARCHAR(255)**)  
  - Unique identifier for the product being sold.

- `Quantity` (**INT**)  
  - Number of units purchased in this transaction.

- `UnitPrice` (**FLOAT**)  
  - The price per unit of the product at the time of sale.

- `TotalAmount` (**FLOAT**)  
  - Total sales amount for this transaction (`Quantity * UnitPrice`).

- `State` (**VARCHAR(255)**)  
  - The state (region) where the transaction occurred.

- `Latitude` (**FLOAT**)  
  - The geographical latitude coordinate of the transaction location.

- `Longitude` (**FLOAT**)  
  - The geographical longitude coordinate of the transaction location.

#### **SQL Query Constraints:**
- **Primary Key**: `ID` uniquely identifies each transaction.
- **Aggregation**: `SUM(TotalAmount)`, `AVG(UnitPrice)`, and `COUNT(ID)` are commonly used.
- **Grouping**: Data is often grouped by `State`, `ProductID`, or `Timestamp` for analysis.

---

### **AI Training Instructions**
1. Always include **only valid column names** in queries.
2. When using `GROUP BY`, only include **non-aggregated columns**.
3. When using **aggregate functions (SUM, AVG, COUNT)**, do not include `ID`, `Timestamp`, `Latitude`, or `Longitude` unless needed.
4. Ensure queries comply with **MySQL’s `ONLY_FULL_GROUP_BY` mode**.
5. If filtering by date, use `WHERE Timestamp BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'`.
6. Use **parameterized queries** to prevent SQL injection.

---

### **Example Queries for Reference**
#### ✅ **Correct Query (Finding Total Sales Per State)**
```sql
SELECT State, SUM(TotalAmount) AS TotalSales
FROM transactions
GROUP BY State
ORDER BY TotalSales DESC;

"""
# Function to get database connection


def get_db_connection():
    """Function to get a database connection."""
    try:
        mydb = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return mydb
    except Error as e:
        print(f"Error: {e}")
        return None


# Function to query Ollama
def query_ollama_cli_humanize(question, db_response):
    """
    Generates a concise, human-readable summary from the database response, 
    returning the final answer between <<< and >>>.
    """

    try:
        prompt = (
            "You are a chatbot AI that generates extremely short answers based ONLY on the database response.\n"
            "Strictly follow these rules:\n"
            "1. The answer must be under 15 words.\n"
            "2. Use only the exact numbers from the database response.\n"
            "3. Do not add explanations, introductions, or extra details.\n"
            "4. Enclose your final answer between <<< and >>>.\n"
            "\n"
            "Example Format:\n"
            "  - Correct: <<< The total sales is $687.77 >>>\n\n"
            f"Database response: {db_response}\n\n"
            f"User's question: {question}\n\n"
            "### Answer (strictly follow the format and length constraints):"
        )

        response = chat(model='deepseek-r1:8b', messages=[
            {'role': 'user', 'content': prompt},
        ])

        humanized_response = response['message']['content'].strip()

        # 1) If the response doesn't contain <<< >>>, enforce it
        if "<<<" not in humanized_response or ">>>" not in humanized_response:
            # Extract up to 15 words
            words = humanized_response.split()
            short_response = " ".join(words[:15])
            humanized_response = f"<<< {short_response} >>>"

        # 2) Ensure the final answer is under 15 words even if AI ignores instructions
        #    We'll parse out only the text between <<< and >>>, then truncate if needed.
        import re
        match = re.search(r'<<<(.*?)>>>', humanized_response, re.DOTALL)
        if match:
            # Get the text inside <<< >>>
            inner_text = match.group(1).strip()
            words = inner_text.split()
            if len(words) > 15:
                inner_text = " ".join(words[:15])  # Truncate to 15 words
            humanized_response = f"<<< {inner_text} >>>"
        else:
            # If for some reason there's no match, wrap forcibly
            words = humanized_response.split()
            short_response = " ".join(words[:15])
            humanized_response = f"<<< {short_response} >>>"

        # 3) Handle "not specified"/"no data" in case AI tries to generate a negative statement
        if "not specified" in humanized_response.lower() or "no data" in humanized_response.lower():
            # Provide a default if the DB has a numeric field named CombinedTotal
            if db_response and isinstance(db_response, list) and "CombinedTotal" in db_response[0]:
                amount = float(db_response[0]["CombinedTotal"])
                humanized_response = f"<<< The combined total sales amount is ${amount:.2f} >>>"
            else:
                humanized_response = "<<< Sorry, not enough data to form a short answer >>>"

        return humanized_response

    except Exception as e:
        print(f"Error: {e}")
        return "<<< Sorry, I couldn't process the data into a human-readable format. >>>"

    """Takes the database response and generates a human-readable summary using the question for context."""
    try:
        prompt = (
            f"You are an AI assistant that converts database query results into easy-to-understand answers.\n\n"
            f"User asked: {question}\n\n"
            f"Database returned the following JSON data:\n{db_response}\n\n"
            f"Please analyze the response and provide a clear, human-readable summary.\n"
            f"Keep it concise and informative. Do not include any SQL-related details."
        )

        response = chat(model='deepseek-r1:8b', messages=[
            {'role': 'user', 'content': prompt},
        ])

        humanized_response = response['message']['content'].strip()
        return humanized_response
    except Exception as e:
        print(f"Error: {e}")
        return "Sorry, I couldn't process the data into a human-readable format."


def query_ollama_cli(question):
    """Query Ollama to generate a MySQL-compliant SQL query."""
    try:
        prompt = (
            f"You are an SQL Jedi so make no mistakes, Convert the following question into a valid MySQL query that retrieves the required data "
            f"from the transactions table. Make sure it is compatible with `ONLY_FULL_GROUP_BY` mode. "
            f"Ensure that all non-aggregated columns appear in `GROUP BY` and avoid errors. "
            f"Table Structure:\n{TABLE_CONTEXT} only use the columns provided in {TABLE_CONTEXT}\n\n"
            f"Do NOT include `TotalAmount` in `GROUP BY` when using aggregate functions.\n\n"
            f"Database: MySQL 8.0\n\n"
            f"User's Question: {question}\n\n"
            "Provide only the SQL query, enclosed in triple backticks (```sql ... ```). "
        )

        response = chat(model='deepseek-r1:8b',
                        messages=[{'role': 'user', 'content': prompt}])
        sql_query = extractSQLQuery(response['message']['content'])
        return sql_query
    except Exception as e:
        print(f"Error: {e}")
        return None


def query_ollama_cli_forError(old_sql, error_message):
    """Query Ollama to fix the SQL query based on an error message."""
    try:
        prompt = (
            f"The following SQL query resulted in an error:\n\n"
            f"SQL Query:\n{old_sql}\n\n"
            f"Error Message:\n{error_message}\n\n"
            "Please provide a corrected SQL query that resolves the error. "
            "Ensure that the corrected query maintains the exact table structure as defined below:\n\n"
            f"Table Structure:\n{TABLE_CONTEXT} only use the columns provided in {TABLE_CONTEXT}\n\n"
            f"Database: MySQL 8.0\n\n"
            "Provide only the corrected SQL query enclosed within triple backticks (` ```sql ... ``` `). "
            "Do not include any explanations, headers, or comments.\n\n"
            "Corrected SQL Query:\n"
        )

        response = chat(model='deepseek-r1:8b', messages=[
            {'role': 'user', 'content': prompt},
        ])

        sql_query = extractSQLQuery(response['message']['content'])
        return sql_query
    except Exception as e:
        print(f"Error: {e}")
        return None

# Function to execute SQL query


def execute_sql_query(sql_query):
    """Execute the SQL query and return the results."""
    try:
        connection = get_db_connection()
        if connection is None:
            return None, "Failed to connect to the database."

        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql_query)
        results = cursor.fetchall()

        cursor.close()
        connection.close()

        return results, None
    except Error as e:
        print(f"Database error: {e}")
        return None, str(e)


# Function to format results
def format_results(results, expected_columns):
    """Format the results to match the expected table structure."""
    if not results:
        return []

    formatted = []
    for row in results:
        formatted_row = {column: row.get(column, None)
                         for column in expected_columns}
        formatted.append(formatted_row)

    return formatted


EXPECTED_COLUMNS = ['id', 'transaction_date', 'amount',
                    'category']  # Adjust based on your table structure


# Endpoint to set the number of records to return
@app.route('/api/set-return-num/<int:num>', methods=['GET'])
def set_return_num(num):
    global return_data_num
    return_data_num = num
    return jsonify({'message': f'Successfully set the number of records to return to {num}'})


# @app.route('/ask', methods=['POST'])


def extractSQLQuery(text):
    """
    Extracts the SQL query from a text response.
    It handles cases where the SQL query is enclosed within triple backticks
    or directly present in the text.

    Args:
        text (str): The text response containing the SQL query.

    Returns:
        str: The extracted SQL query.

    Raises:
        ValueError: If no valid SQL query is found in the response.
    """
    logging.info(f"Extracting SQL Query from text: {text}")

    # First attempt: Extract SQL within triple backticks
    code_block_pattern = re.compile(
        r"```sql\s*([\s\S]*?)\s*```", re.IGNORECASE)
    match = code_block_pattern.search(text)
    if match:
        sql_query = match.group(1).strip()
        logging.info("SQL query extracted from code block.")
        return sql_query

    # Second attempt: Extract SQL directly present in the text
    # Look for a line starting with SELECT and ending with a semicolon
    direct_sql_pattern = re.compile(r"(SELECT[\s\S]*?;)", re.IGNORECASE)
    match = direct_sql_pattern.search(text)
    if match:
        sql_query = match.group(1).strip()
        logging.info("SQL query extracted directly from text.")
        return sql_query

    # Optional Third Attempt: Handle SQL without a terminating semicolon
    # This can be useful if the SQL query is the last part of the text without a semicolon
    direct_sql_no_semicolon_pattern = re.compile(
        r"(SELECT[\s\S]*)", re.IGNORECASE)
    match = direct_sql_no_semicolon_pattern.search(text)
    if match:
        sql_query = match.group(1).strip()
        # Additional validation can be performed here
        if sql_query.endswith(";"):
            sql_query = sql_query[:-1].strip()
        logging.info(
            "SQL query extracted directly from text without semicolon.")
        return sql_query

    # If all attempts fail, raise an error
    logging.error("No valid SQL query found in the response.")
    raise ValueError("No valid SQL query found in the response.")


def ask_question(question):

    print("Received POST requestsss", question)
    if not question:
        return jsonify({'error': 'No question provided.'}), 400

    sql_query = query_ollama_cli(question)
    print("sql query", sql_query)
    sql_query_sanitized = extractSQLQuery(sql_query)
    print(f"Generated SQL Query: {sql_query_sanitized}")

    # print(f"Generated SQL Query: {sql_query}")

    if not sql_query:
        return jsonify({'error': 'Failed to generate SQL query.'}), 500

    results, error = execute_sql_query(sql_query_sanitized)

    print(f"Results: {results}")

    if error:
        new_sql_query = query_ollama_cli_forError(sql_query, error)
        new_sql_query_sanitized = new_sql_query
        print(f"Generated SQL Query: {new_sql_query_sanitized}")
        results, error = execute_sql_query(sql_query_sanitized)
        return jsonify({'error': f"SQL Execution Error: {error}"}), 500

    return results

    formatted_data = format_results(results, EXPECTED_COLUMNS)

    # return jsonify({'data': formatted_data}), 200, {'Access-Control-Allow-Origin': '*'}


@app.route('/api/ask-database-set/<string:question>', methods=['GET'])
def ask_database_set(question):
    global Question
    Question = question

    print("Question is asked", question)
    question = question
    sql_query_analysed = should_reload()

    if not sql_query_analysed:
        return jsonify({'error': 'Failed to generate SQL query.'}), 500

    return jsonify({'message': f'Query generated: {sql_query_analysed}'}), 200


@app.route('/api/ask-database/<int:num>', methods=['GET'])
def ask_database():
    print("question is asked", )
    ollama_response = should_reload()
    print('finally response from ollama', ollama_response)

    return jsonify({'ollama_response': ollama_response}), 200
# Function to determine if client should reload data


@app.route('/api/should-reload', methods=['GET'])
def should_reload():
    print("Question from should reload", Question)
    global last_checked_data
    connection = get_db_connection()
    if connection is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    cursor = connection.cursor(dictionary=True)
    try:
        # Get the latest transaction
        cursor.execute("SELECT * FROM transactions ORDER BY ID DESC LIMIT 1")
        latest_record = cursor.fetchone()

        # Get the total number of records
        cursor.execute("SELECT COUNT(*) AS count FROM transactions")
        total_records = cursor.fetchone()
        count = total_records['count']

        cursor.close()
        connection.close()

        if last_checked_data != count:
            last_checked_data = count

            # Prepare a question for Ollama

            # Query Ollama
            ollama_response = ask_question(Question)
            print('ollama response', ollama_response)

            reload_decision = True
            if Question.lower().startswith("give me"):
                global return_this_data
                return_this_data = ollama_response
            else:
                ollama_humanize = query_ollama_cli_humanize(
                    ollama_response, Question
                )
                return ollama_humanize

        return jsonify({'reload': False}), 200, {'Access-Control-Allow-Origin': '*'}

    except Error as e:
        print(f"Error: {e}")
        return jsonify({'error': 'Database query failed'}), 500


# API to fetch the last N records
@app.route('/api/last-10-records', methods=['GET'])
def get_last_10_records():
    return jsonify(return_this_data)


# Run Flask app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3308)
