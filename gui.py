from psycopg2 import OperationalError
from tkinter import *
from tkinter import ttk
from tkinter.messagebox import showinfo
import mysql.connector
import psycopg2
from mysql.connector import Error

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

def get_tables():
    cursor.execute("SELECT metadata.table.name "
                   "FROM metadata.table "
                   "JOIN metadata.database "
                   "ON metadata.table.database_id = metadata.database.database_id "
                   "WHERE metadata.database.name = '" + databases_combobox.get() + "'")
    tables = cursor.fetchall()
    tables_combobox.set("")
    select1_combobox.set("")
    select2_combobox.set("")
    select3_combobox.set("")
    where_combobox.set("")
    select1_combobox.config(values=[])
    select2_combobox.config(values=[])
    select3_combobox.config(values=[])
    where_combobox.config(values=[])
    from2_label.config(text="")
    tables_combobox.config(values=tables)

def get_attributes():
    cursor.execute("SELECT metadata.attribute.name "
                   "FROM metadata.attribute "
                   "JOIN metadata.table "
                   "ON metadata.attribute.table_id = metadata.table.table_id "
                   "WHERE metadata.table.name = '" + tables_combobox.get() + "'")
    attributes = cursor.fetchall()
    select1_combobox.set("")
    select2_combobox.set("")
    select3_combobox.set("")
    where_combobox.set("")
    select1_combobox.config(values=attributes)
    select2_combobox.config(values=attributes)
    select3_combobox.config(values=attributes)
    where_combobox.config(values=attributes)
    from2_label.config(text=tables_combobox.get())

def execute():
    rows = []
    database = databases_combobox.get()
    table = tables_combobox.get()
    select = []
    if select1_combobox.get() != "":
        select.append(select1_combobox.get())
    if select2_combobox.get() != "":
        select.append(select2_combobox.get())
    if select3_combobox.get() != "":
        select.append(select3_combobox.get())
    where = []
    if where_combobox.get() != "":
        where.append(where_combobox.get())
    if where1_entry.get() != "":
        where.append(where1_entry.get())
    if where1_entry.get() != "":
        where.append(where2_entry.get())
    file = open('Queries.txt', 'a')

    if database == "sakila":
        sakila_connection = create_mysql_connection("31.129.104.17", "root", "13221", "3326")
        try:
            with sakila_connection:
                with sakila_connection.cursor() as sakila_cursor:
                    if table != '':
                        if len(select) == 0:
                            cursor.execute("SELECT metadata.attribute.name "
                                           "FROM metadata.attribute "
                                           "JOIN metadata.table "
                                           "ON metadata.attribute.table_id = metadata.table.table_id "
                                           "WHERE metadata.table.name = '" + table + "'")
                            select = cursor.fetchall()
                            if len(where) == 0:
                                file.write("MySQL DBMS: \n")
                                sakila_cursor.execute("SELECT * "
                                               "FROM sakila." + table)
                                file.write("SELECT * "
                                           "FROM sakila." + table + '\n\n')
                                file.close()
                                rows = sakila_cursor.fetchall()
                            elif len(where) == 3:
                                file.write("MySQL DBMS: \n")
                                sakila_cursor.execute("SELECT * "
                                               "FROM sakila." + table + " "
                                               "WHERE sakila." + table + "." + where[0] + " "
                                               "BETWEEN " + where[1] + " AND " + where[2])
                                file.write("SELECT * "
                                           "FROM sakila." + table + " "
                                           "WHERE sakila." + table + "." + where[0] + " "
                                           "BETWEEN " +
                                           where[1] + " AND " + where[2] + '\n\n')
                                file.close()
                                rows = sakila_cursor.fetchall()
                            else:
                                showinfo(title="Info", message="Incorrect WHERE condition")
                                return -1
                        else:
                            str = ""
                            for i in range(len(select) - 1):
                                str += select[i] + ', '
                            str += select[len(select) - 1]
                            if len(where) == 0:
                                file.write("MySQL DBMS: \n")
                                sakila_cursor.execute("SELECT " + str + " "
                                               "FROM sakila." + table)
                                file.write("SELECT " + str + " "
                                           "FROM sakila." + table + '\n\n')
                                file.close()
                                rows = sakila_cursor.fetchall()
                            elif len(where) == 3:
                                file.write("MySQL DBMS: \n")
                                sakila_cursor.execute("SELECT " + str + " "
                                               "FROM sakila." + table + " "
                                               "WHERE sakila." + table + "." + where[0] + " "
                                               "BETWEEN " + where[1] + " AND " + where[2])
                                file.write("SELECT " + str + " "
                                           "FROM sakila." + table + " "
                                           "WHERE sakila." + table + "." + where[0] + " "
                                           "BETWEEN " + where[1] + " AND " + where[2] + '\n\n')
                                file.close()
                                rows = sakila_cursor.fetchall()
                            else:
                                showinfo(title="Info", message="Incorrect WHERE condition")
                                return -1
                    else:
                        showinfo(title="Info", message="Table is not chosen")
                        return -1
        except OperationalError as e:
            print(f"The error '{e}' occurred")
    elif database == "demo":
        pg_connection = create_pg_connection(database, "postgres", "13221", "31.129.104.17", "5433")
        try:
            with pg_connection:
                with pg_connection.cursor() as pg_cursor:
                    if table != '':
                        if len(select) == 0:
                            cursor.execute("SELECT metadata.attribute.name "
                                           "FROM metadata.attribute "
                                           "JOIN metadata.table "
                                           "ON metadata.attribute.table_id = metadata.table.table_id "
                                           "WHERE metadata.table.name = '" + table + "'")
                            select = cursor.fetchall()
                            if len(where) == 0:
                                file.write("Postgres DBMS: \n")
                                pg_cursor.execute("SELECT * "
                                               "FROM " + table)
                                file.write("SELECT * "
                                           "FROM " + table + '\n\n')
                                file.close()
                                rows = pg_cursor.fetchall()
                            elif len(where) == 3:
                                file.write("Postgres DBMS: \n")
                                pg_cursor.execute("SELECT * "
                                               "FROM " + table + " "
                                               "WHERE " + table + "." + where[0] + " "
                                               "BETWEEN " + where[1] + " AND " + where[2])
                                file.write("SELECT * "
                                           "FROM " + table + " "
                                           "WHERE " + table + "." + where[0] + " "
                                           "BETWEEN " + where[1] + " AND " + where[2] + '\n\n')
                                file.close()
                                rows = pg_cursor.fetchall()
                            else:
                                showinfo(title="Info", message="Incorrect WHERE condition")
                                return -1
                        else:
                            str = ""
                            for i in range(len(select) - 1):
                                str += select[i] + ', '
                            str += select[len(select) - 1]
                            if len(where) == 0:
                                file.write("Postgres DBMS: \n")
                                pg_cursor.execute("SELECT " + str + " "
                                                  "FROM " + table)
                                file.write("SELECT " + str + " "
                                           "FROM " + table + '\n\n')
                                file.close()
                                rows = pg_cursor.fetchall()
                            elif len(where) == 3:
                                file.write("Postgres DBMS: \n")
                                pg_cursor.execute("SELECT " + str + " "
                                               "FROM " + table + " "
                                               "WHERE " + table + "." + where[0] + " "
                                               "BETWEEN " + where[1] + " AND " + where[2])
                                file.write("SELECT " + str + " "
                                          "FROM " + table + " "
                                          "WHERE " + table + "." + where[0] + " "
                                          "BETWEEN " + where[1] + " AND " + where[2] + '\n\n')
                                file.close()
                                rows = pg_cursor.fetchall()
                            else:
                                showinfo(title="Info", message="Incorrect WHERE condition")
                                return -1
                    else:
                        showinfo(title="Info", message="Table is not chosen")
                        return -1
        except OperationalError as e:
            print(f"The error '{e}' occurred")
    else:
        showinfo(title="Info", message="Database is not chosen")
        return -1
    for i in result.get_children():
        result.delete(i)
    result.config(columns=select)
    for i in select:
        result.heading(i, text=i)
    for row in rows:
        result.insert("", END, values=row)

metadata_connection = create_mysql_connection("31.129.104.17", "root", "13221", "3316")
databases = []
tables = []
attributes = []

try:
    with metadata_connection:
        with metadata_connection.cursor() as cursor:
            root = Tk()
            root.title("SELECT")
            root.geometry("900x750")

            cursor.execute("SELECT name FROM metadata.database")
            databases = cursor.fetchall()

            databases_label = Label(text="Choose the database:", font=("Arial", 14), justify='left')
            databases_label.place(x=20, y=20)
            databases_combobox = ttk.Combobox(values=databases, state="readonly")
            databases_combobox.place(x=220, y=24)

            databases_btn = ttk.Button(text="OK", command=get_tables)
            databases_btn.place(x=370, y=22)

            tables_lable = Label(text="Choose the table:", font=("Arial", 14), justify='left')
            tables_lable.place(x=20, y=60)
            tables_combobox = ttk.Combobox(values=tables, state="readonly")
            tables_combobox.place(x=180, y=64)

            tables_btn = ttk.Button(text="OK", command=get_attributes)
            tables_btn.place(x=330, y=62)

            select_label = Label(text="SELECT", font=("Arial", 14), justify='left')
            select_label.place(x=20, y=130)
            select1_combobox = ttk.Combobox(values=attributes, state="readonly")
            select1_combobox.place(x=110, y=134)
            select2_combobox = ttk.Combobox(values=attributes, state="readonly")
            select2_combobox.place(x=260, y=134)
            select3_combobox = ttk.Combobox(values=attributes, state="readonly")
            select3_combobox.place(x=410, y=134)

            from1_label = Label(text="FROM", font=("Arial", 14), justify='left')
            from1_label.place(x=20, y=170)
            from2_label = Label(text="", font=("Arial", 14), justify='left')
            from2_label.place(x=110, y=170)

            where1_label = Label(text="WHERE", font=("Arial", 14), justify='left')
            where1_label.place(x=20, y=210)
            where_combobox = ttk.Combobox(values=attributes, state="readonly")
            where_combobox.place(x=110, y=214)
            where2_label = Label(text="BETWEEN", font=("Arial", 14), justify='left')
            where2_label.place(x=260, y=210)
            where1_entry = ttk.Entry()
            where1_entry.place(x=370, y=214)
            where3_label = Label(text="AND", font=("Arial", 14), justify='left')
            where3_label.place(x=520, y=210)
            where2_entry = ttk.Entry()
            where2_entry.place(x=570, y=214)

            result = ttk.Treeview(columns=attributes, show="headings")
            result.place(x=20, y=250, height=420, width=800)
            scrollbar = ttk.Scrollbar(orient=VERTICAL, command=result.yview)
            scrollbar.place(x=820, y=250, height=420)
            result.configure(yscroll=scrollbar.set)

            execute_btn = ttk.Button(text="Execute", command=execute)
            execute_btn.place(x=20, y=700)

            root.mainloop()

except OperationalError as e:
    print(f"The error '{e}' occurred")
