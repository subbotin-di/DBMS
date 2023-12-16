import mysql.connector
import psycopg2
from mysql.connector import Error
from psycopg2 import OperationalError

def create_mysql_connection(host_name, user_name, user_password, port):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            port=port
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def create_pg_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def get_mysql_metadata(db):
    connection = create_mysql_connection("sakila", "root", "13221", "3306")
    tables = []
    attributes = []
    keys = []
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + db + "'")
                tables = cursor.fetchall()
                connection.commit()
                for i in range(len(tables)):
                    cursor.execute("select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS "
                                   "where TABLE_SCHEMA='" + db + "' and TABLE_NAME='" + tables[i][0] + "'")
                    attributes.append(cursor.fetchall())
                    connection.commit()
                for i in range(len(tables)):
                    cursor.execute("SELECT CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
                                   "REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, "
                                   "REFERENCED_COLUMN_NAME "
                                   "FROM information_schema.KEY_COLUMN_USAGE "
                                   "WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME = '" + tables[i][0] + "' "
                                   "AND REFERENCED_TABLE_SCHEMA IS NOT NULL")
                    keys.append(cursor.fetchall())
                    connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")
    connection = create_mysql_connection("metadata", "root", "13221", "3306")
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO metadata.database (name) "
                               "VALUES ('" + db + "')")
                connection.commit()
                for i in range(len(tables)):
                    cursor.execute("INSERT INTO metadata.table (database_id, name) "
                                   "SELECT database_id, '" + tables[i][0] + "' "
                                   "FROM metadata.database "
                                   "ORDER BY database_id DESC LIMIT 1")
                    connection.commit()
                    for j in range(len(attributes[i])):
                        cursor.execute("INSERT INTO metadata.attribute (table_id, name, type) "
                                       "SELECT table_id, '" + attributes[i][j][0] + "', '" + attributes[i][j][1] + "' "
                                       "FROM metadata.table "
                                       "ORDER BY table_id DESC LIMIT 1")
                        connection.commit()
                    for j in range(len(keys[i])):
                        if len(keys[i]) != 0:
                            cursor.execute("INSERT INTO metadata.key (attribute1_id, attribute2_id, key_code) "
                                           "SELECT k.attribute_id AS attribute1_id, a.attribute_id AS attribute2_id, "
                                           "CONSTRAINT_NAME "
                                           "FROM metadata.attribute AS a, "
                                           "    (SELECT CONSTRAINT_NAME, k.attribute_id, t.table_id, REFERENCED_COLUMN_NAME "
                                           "	FROM metadata.table AS t, "
                                           "		(SELECT CONSTRAINT_NAME, k.database_id, a.attribute_id, "
                                           "        REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                           "		FROM metadata.attribute AS a, "
                                           "			(SELECT CONSTRAINT_NAME, k.database_id, t.table_id, "
                                           "                COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                           "			FROM metadata.table AS t, "
                                           "				(SELECT CONSTRAINT_NAME, d.database_id, TABLE_NAME, COLUMN_NAME, "
                                           "                REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                           "				FROM metadata.database AS d, "
                                           "                (SELECT '" + keys[i][j][0] + "' AS CONSTRAINT_NAME, "
                                            "               '" + keys[i][j][1] + "' AS TABLE_SCHEMA, "
                                            "               '" + keys[i][j][2] + "' AS TABLE_NAME, "
                                            "               '" + keys[i][j][3] + "' AS COLUMN_NAME, "
                                            "               '" + keys[i][j][4] + "' AS REFERENCED_TABLE_SCHEMA, "
                                            "               '" + keys[i][j][5] + "' AS REFERENCED_TABLE_NAME, "
                                            "               '" + keys[i][j][6] + "' AS REFERENCED_COLUMN_NAME) AS k "
                                           "				WHERE d.name = k.TABLE_SCHEMA) AS k "
                                           "			WHERE t.name = k.TABLE_NAME "
                                           "			AND t.database_id = k.database_id) AS k "
                                           "		WHERE a.name = k.COLUMN_NAME "
                                           "		AND a.table_id = k.table_id) AS k "
                                          "	WHERE t.name = k.REFERENCED_TABLE_NAME "
                                          "	AND t.database_id = k.database_id) AS k "
                                          "WHERE a.name = k.REFERENCED_COLUMN_NAME "
                                          "AND a.table_id = k.table_id")
                            connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")

def get_pg_metadata(db):
    pg_connection = create_pg_connection(db, "postgres", "13221", "demo", "5432")
    attributes = []
    keys = []
    try:
        with pg_connection:
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.execute("SELECT table_name FROM information_schema.tables "
                                  "WHERE table_schema NOT IN ('information_schema','pg_catalog')")
                tables = pg_cursor.fetchall()
                for i in range(len(tables)):
                    pg_cursor.execute("select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS "
                                    "where TABLE_SCHEMA NOT IN ('information_schema','pg_catalog') "
                                    "and TABLE_NAME='" + tables[i][0] + "'")
                    attributes.append(pg_cursor.fetchall())
                    pg_cursor.execute("SELECT k1.constraint_name, k1.table_name, k1.column_name, "
                                      "k2.table_name AS referenced_table_name, k2.column_name AS referenced_column_name "
                                      "FROM information_schema.key_column_usage k1 "
                                      "JOIN information_schema.referential_constraints fk "
                                      "USING (constraint_schema, constraint_name) "
                                      "JOIN information_schema.key_column_usage k2 "
                                      "ON k2.constraint_schema = fk.unique_constraint_schema "
                                      "AND k2.constraint_name = fk.unique_constraint_name "
                                      "AND k2.ordinal_position = k1.position_in_unique_constraint;")
                    keys = pg_cursor.fetchall()
    except Error as e:
        print(f"The error '{e}' occurred")
    connection = create_mysql_connection("metadata", "root", "13221", "3306")
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO metadata.database (name) "
                               "VALUES ('" + db + "')")
                connection.commit()
                for i in range(len(tables)):
                    cursor.execute("INSERT INTO metadata.table (database_id, name) "
                                   "SELECT database_id, '" + tables[i][0] + "' "
                                   "FROM metadata.database "
                                   "ORDER BY database_id DESC LIMIT 1")
                    connection.commit()
                    for attribute in attributes[i]:
                        cursor.execute("INSERT INTO metadata.attribute (table_id, name, type) "
                                       "SELECT table_id, '" + attribute[0] + "', '" + attribute[1] + "' "
                                       "FROM metadata.table "
                                       "ORDER BY table_id DESC LIMIT 1")
                        connection.commit()
                for key in keys:
                    cursor.execute("INSERT INTO metadata.key (attribute1_id, attribute2_id, key_code) "
                                   "SELECT k.attribute_id AS attribute1_id, a.attribute_id AS attribute2_id, "
                                   "CONSTRAINT_NAME "
                                   "FROM metadata.attribute AS a, "
                                   "    (SELECT CONSTRAINT_NAME, k.attribute_id, t.table_id, REFERENCED_COLUMN_NAME "
                                   "	FROM metadata.table AS t, "
                                   "		(SELECT CONSTRAINT_NAME, k.database_id, a.attribute_id, "
                                   "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                   "		FROM metadata.attribute AS a, "
                                   "			(SELECT CONSTRAINT_NAME, k.database_id, t.table_id, "
                                   "COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                   "			FROM metadata.table AS t, "
                                   "				(SELECT CONSTRAINT_NAME, d.database_id, TABLE_NAME, COLUMN_NAME, "
                                   "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                                   "				FROM metadata.database AS d, "
                                   "					(SELECT '" + key[0] + "' AS CONSTRAINT_NAME, '" + db + "' "
                                                        "AS TABLE_SCHEMA, '" + key[1] + "' AS TABLE_NAME, "
                                                        "'" + key[2] + "' AS COLUMN_NAME, '" + db + "' "
                                                        "AS REFERENCED_TABLE_SCHEMA, '" + key[3] + "' "
                                                        "AS REFERENCED_TABLE_NAME, '" + key[4] + "' "
                                                        "REFERENCED_COLUMN_NAME) AS k "
                                    "				WHERE d.name = k.TABLE_SCHEMA) AS k "
                                    "			WHERE t.name = k.TABLE_NAME "
                                    "			AND t.database_id = k.database_id) AS k "
                                    "		WHERE a.name = k.COLUMN_NAME "
                                    "		AND a.table_id = k.table_id) AS k "
                                    "	WHERE t.name = k.REFERENCED_TABLE_NAME "
                                    "	AND t.database_id = k.database_id) AS k "
                                    "WHERE a.name = k.REFERENCED_COLUMN_NAME "
                                    "AND a.table_id = k.table_id")
                    connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")

if __name__ == '__main__':
    get_pg_metadata("demo")
    get_mysql_metadata("sakila")

