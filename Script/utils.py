import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text

from exceptions import AppException


class SalaryDatabase:
    def __init__(self, user, password, host, database, port=3306):
        self.mysql_connection = {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
        }

    def setup_database(self):
        """
        connect to mysql and setup database and create table
        """
        try:
            conn = mysql.connector.connect(
                user=self.mysql_connection['user'],
                password=self.mysql_connection['password'],
                host=self.mysql_connection['host'],
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_connection['database']}")
            cursor.execute(f"USE {self.mysql_connection['database']}")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Employees (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    role VARCHAR(255) NOT NULL,
                    salary INT
                ) AUTO_INCREMENT=1;
            ''')
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error setting up database: {e}")

    def insert_employee_data(self, employee_data):
        """
        inserts data into the table
        :param employee_data: employee details
        """
        try:
            database_url = f"mysql+mysqlconnector://{self.mysql_connection['user']}:{self.mysql_connection['password']}@{self.mysql_connection['host']}:{self.mysql_connection['port']}/{self.mysql_connection['database']}"
            engine = create_engine(database_url, echo=True)
            connection = engine.connect()
            insert_query = text("""
                INSERT INTO Employees (id, name, role, salary)
                VALUES (%s, %s, %s, %s)
            """)
            for data in employee_data:
                connection.execute(insert_query, data)
            connection.close()
            engine.dispose()
        except Exception as e:
            raise AppException(f"Error inserting employee data: {e}")

    def calculate_average_salary_per_role(self):
        """
        calculates average salary of every employee by role
        """
        try:
            database_url = f"mysql+mysqlconnector://{self.mysql_connection['user']}:{self.mysql_connection['password']}@{self.mysql_connection['host']}:{self.mysql_connection['port']}/{self.mysql_connection['database']}"
            engine = create_engine(database_url, echo=True)
            connection = engine.connect()
            average_salary_per_role_query = text("""
                SELECT role, AVG(salary) AS average_salary
                FROM Employees
                GROUP BY role
            """)
            df_average_salary_per_role = pd.read_sql_query(average_salary_per_role_query, connection)
            connection.close()
            engine.dispose()
            return df_average_salary_per_role
        except Exception as e:
            print(f"Error calculating average salary per role: {e}")

    def get_all_employees_sorted_by_salary(self):
        """
        sorts the salary of every employee in descending order
        """
        try:
            database_url = f"mysql+mysqlconnector://{self.mysql_connection['user']}:{self.mysql_connection['password']}@{self.mysql_connection['host']}:{self.mysql_connection['port']}/{self.mysql_connection['database']}"
            engine = create_engine(database_url, echo=True)
            connection = engine.connect()
            all_employees_query = text("""
                SELECT id, name, role, salary
                FROM Employees
                ORDER BY salary DESC
            """)
            df_all_employees = pd.read_sql_query(all_employees_query, connection)
            connection.close()
            engine.dispose()
            return df_all_employees
        except Exception as e:
            print(f"Error retrieving all employees: {e}")

    def display_merged_data(self):
        """
        displays the final data in the form of dataframe
        """
        try:
            df_average_salary_per_role = self.calculate_average_salary_per_role()
            df_all_employees = self.get_all_employees_sorted_by_salary()
            df = pd.merge(df_all_employees, df_average_salary_per_role, on='role')
            print(df)
        except Exception as e:
            print(f"Error displaying merged data: {e}")
