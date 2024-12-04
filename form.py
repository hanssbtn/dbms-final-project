import matplotlib.pyplot as plt
from pyodbc import Row, Cursor, Connection, connect
from typing import *
from IPython.display import clear_output
from concurrent import futures
import statistics
import gc
import numpy as np
from datetime import datetime
plt.rcParams["font.size"] = 7

DB_DRIVER = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path};"
DB_FILE = "C:/Users/Student/Documents/DBMS FINAL PROJECT.accdb"
month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

def execute_query(query: str, *parameters) -> list[Row]:
    db = connect(DB_DRIVER.format(path = DB_FILE))
    cursor = db.cursor()

    result = db.execute(query, parameters).fetchall()

    cursor.close()
    db.close()
    
    return result

queries = {
    "get_current_year" : """
    SELECT DISTINCT YEAR(orders.order_date) AS current_year
    FROM orders
    """,

    "get_current_month" : """
    SELECT DISTINCT MONTH(orders.order_date) AS current_month
    FROM orders
    WHERE YEAR(orders.order_date) = ?
    """,

    "get_sold_items" : """
    SELECT 
    DISTINCT menu_items.item_name
    FROM menu_items, orders, order_details
    WHERE YEAR(orders.order_date) = ? AND
    MONTH(orders.order_date) BETWEEN ? AND ? AND
    menu_items.menu_item_id = order_details.menu_item_id AND
    orders.order_id = order_details.order_id
    """,

    "get_quantities_sold" : """
    SELECT 
    menu_items.item_name,
    COUNT(menu_items.menu_item_id) AS quantities_sold
    FROM menu_items, orders, order_details
    WHERE YEAR(orders.order_date) = ? AND
    MONTH(orders.order_date) BETWEEN ? AND ? AND
    menu_items.item_name IN ( ? ) AND
    menu_items.menu_item_id = order_details.menu_item_id AND
    orders.order_id = order_details.order_id
    GROUP BY menu_items.item_name;
    """,

    "get_sales" : """
    SELECT
    MONTH(orders.order_date) AS current_month,
    SUM(menu_items.price) AS total_sales
    FROM menu_items, orders, order_details
    WHERE YEAR(orders.order_date) = ? AND
    MONTH(orders.order_date) BETWEEN ? AND ? AND
    menu_items.item_name = ? AND
    menu_items.menu_item_id = order_details.menu_item_id AND
    orders.order_id = order_details.order_id
    GROUP BY MONTH(orders.order_date), YEAR(orders.order_date); 
    """
}

def get_years() -> set[int]:
    return set(int(row[0]) for row in execute_query(queries["get_current_year"]))

def get_month(y: int) -> set[int]:
    return set(int(row[0]) for row in execute_query(queries["get_current_month"], y))

def get_items(sm: int, em: int, y: int) -> list[str]:
    return list(row[0] for row in execute_query(queries["get_sold_items"], y, sm, em))

def make_charts() -> None:
    end = False
    while not end:
        year_set = get_years()
        y: int
        while True:
            print("Choose year:")
            print("\n".join([f"{i + 1}: {yr}" for i, yr in enumerate(year_set)]))
            try:
                y = datetime.strptime(input("Enter year (YYYY): "), "%Y").year
                if y not in year_set:
                    raise ValueError(f"Data for year {y} is not available.")
                break
            except Exception as e:
                print(f"Please enter a valid year. {e}")
        month_set = get_month(y)


        # with futures.ProcessPoolExecutor() as pool:
        #     chunks = [
        #         pool.submit(execute_query, queries["get_current_year"])
        #     ]
        #     for future in futures.as_completed(chunks):
        #         set_res.add(future.result())

        #     del chunks

        start_month: int
        end_month: int
        print(month_set)
        while True:
            print("Choose month range separated by (-): ")
            print("\n".join(f"{i + 1}: {month_names[m - 1]}" for i, m in enumerate(month_set)))
            try:
                start_month = int(input(f"Enter start month (1-{len(month_set)}): "))
                if (start_month not in range(1, 13)):
                    raise ValueError(f"Start month number must be between 1-12. (got {start_month})")
                if (start_month not in month_set):
                    raise ValueError(f"Data for {start_month}/{y} is not available.")
                if (start_month == list(month_set)[-1]):
                    end_month = start_month
                    break
                else: 
                    end_month = int(input(f"Enter end month ({start_month}-{len(month_set)}): "))
                    if (end_month not in range(start_month, len(month_set) + 1)):
                        raise ValueError(f"End month number must be between {start_month}-{len(month_set)}. (got {end_month})")
                    if (end_month not in month_set):
                        raise ValueError(f"Data for {end_month}/{y} is not available.")
                break
            except Exception as e:
                print(f"Please enter a valid month range. {e}")

        menu_item_list = get_items(start_month, end_month, y)

        while True: 
            print("Choose item index(es) separated by comma: ")
            print("\n".join(f"{i}: {r}" for i, r in enumerate(menu_item_list)))
            try:
                menu_items = list(set(int(idx.strip()) for idx in input("Enter item indexes: ").split(",")))
                for idx in menu_items:
                    if (idx not in range(-len(menu_item_list), len(menu_item_list))):
                        raise ValueError(f"Item index out of bounds. (got {idx})")
                break
            except Exception as e:
                print(f"Please enter a valid index. {e}")
        
        print(", ".join([f'"{menu_item_list[idx]}"' for idx in menu_items]))
        res = []

        with futures.ProcessPoolExecutor() as pool:
            chunks = [
                pool.submit(execute_query, queries["get_sales"], y, start_month, end_month, menu_item_list[idx]) for idx in menu_items
            ]
            for future in futures.as_completed(chunks):
                res.append(future.result())
            del chunks
        
        figs: list[plt.Figure] = []
        for i, rows in enumerate(res):
            fig = plt.figure(num = i, figsize=(30, 20))
            plt.tight_layout()
            plt.title(f"{menu_item_list[menu_items[i]]} Sales ({start_month}/{y} - {end_month}/{y})")
            plt.xlabel("Months")
            plt.ylabel("Sales (US$)")
            plt.xticks([i for i in range(start_month, end_month + 1)], [month_names[i] for i in range(max(start_month - 1, 0), min(end_month, len(month_names)))])
            plt.xlim(start_month, end_month)
            xval = [row[0] for row in rows]
            yval = [float(row[1]) for row in rows]
            plt.plot(xval, yval)
            for i in range (end_month - start_month + 1):
                plt.annotate(f"${yval[i]:.2f}", (xval[i], yval[i]))
            figs.append(fig)

        for i, f in enumerate(figs):
            while (True):
                f.show()
                match (input("Would you like to save this chart? [y/n] ")).lower():
                    case "y" | "yes":
                        f.savefig(f"{menu_item_list[menu_items[i]]} Sales ({month_names[start_month - 1]} {y} - {month_names[end_month - 1]}-{y}).png")
                        break
                    case "n" | "no" | "q":
                        break
                    case _:
                        print("invalid response. Please try again.")
            f.clear()
            plt.close(f)
            del f
        del figs
        gc.collect(generation=2)
        gc.collect(generation=2)
        end = True
        while (True):
            match (input("Would you like to plot another chart? [y/n] ")).lower():
                case "y" | "yes":
                    
                    break
                case "n" | "no" | "q":
                    break
                case _:
                    print("invalid response. Please try again.")

if __name__ == "__main__":
    make_charts()