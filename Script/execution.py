from Script.utils import SalaryDatabase


def main():
    db_credentials = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'database': 'your_database',
        'port': 3306,
    }

    salary_db = SalaryDatabase(**db_credentials)

    salary_db.setup_database()

    employee_data = [
        (101, "Nupur Khare", "Data Engineer", 900000),
        (102, "Ria Sharma", "Software Engineer", 1000000),
        (103, "Aditya K", "Platform Engineer", 1800000),
        (104, "Shubhi Das", "Solutions Architect", 2500000),
        (105, "John Doe", "System Engineer", 600000),
        (106, "Rishi S", "System Engineer", 500000),
        (107, "Suman K", "Platform Engineer", 2900000),
        (108, "Zain C", "Data Engineer", 1700000),
        (109, "Yuzi L", "Software Engineer", 1300000),
        (110, "Krish N", "Solutions Architect", 2900000)
    ]
    salary_db.insert_employee_data(employee_data)

    average_salary_df = salary_db.calculate_average_salary_per_role()
    print("Average Salary per Role:")
    print(average_salary_df)

    all_employees_df = salary_db.get_all_employees_sorted_by_salary()
    print("\nAll Employees Sorted by Salary:")
    print(all_employees_df)

    print("\nMerged Data:")
    salary_db.display_merged_data()


if __name__ == "__main__":
    main()
