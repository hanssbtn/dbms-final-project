import pyodbc
from concurrent import futures

SQL_DRIVER = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path};"
DB_FILE = "C:/Users/Student/Documents/DBMS FINAL PROJECT.accdb"
CHUNK_SIZE = 50000
LAST_INDEX = 562035
chunk_count = LAST_INDEX // CHUNK_SIZE

queries = [
    """
        SELECT order_date, daily_item_orders.item_name, quantities_sold, (prices.price * quantities_sold) AS daily_sales_from_product
        FROM (SELECT order_date, item_name, COUNT(item_name) AS quantities_sold 
        FROM (order_details INNER JOIN (SELECT menu_item_id, item_name FROM menu_items) AS menus 
        ON order_details.menu_item_id = menus.menu_item_id) 
        INNER JOIN (SELECT order_id, order_date FROM orders) AS order_dates 
        ON order_details.order_id = order_dates.order_id GROUP BY order_date, item_name) AS daily_item_orders, 
        (SELECT item_name, price FROM menu_items) AS prices
        WHERE prices.item_name = daily_item_orders.item_name
        ORDER BY order_date, prices.item_name;
    """,
    """
        SELECT YEAR(order_date) AS current_year, 
        MONTH(order_date) AS current_month, 
        SUM(daily_product_sales.daily_sales_from_product) AS sales
        FROM daily_product_sales
        GROUP BY YEAR(order_date), MONTH(order_date);
    """
]

def read_chunk(offset: int, chunk_size: int) -> list[pyodbc.Row]:
    db: pyodbc.Connection = pyodbc.connect(SQL_DRIVER.format(path=DB_FILE))
    cursor: pyodbc.Cursor = db.cursor().execute("""
        SELECT order_date, daily_item_orders.item_name, quantities_sold, (prices.price * quantities_sold) AS daily_sales_from_product
        FROM (SELECT order_date, item_name, COUNT(item_name) AS quantities_sold FROM (order_details INNER JOIN (SELECT menu_item_id, item_name FROM menu_items) AS menus ON order_details.menu_item_id = menus.menu_item_id) 
        INNER JOIN (SELECT order_id, order_date FROM orders)  
        AS order_dates ON order_details.order_id = order_dates.order_id WHERE order_details_id BETWEEN ? AND ? GROUP BY order_date, item_name) AS daily_item_orders, (SELECT item_name, price FROM menu_items) AS prices
        WHERE prices.item_name = daily_item_orders.item_name
        ORDER BY order_date, prices.item_name
    """, offset, offset + chunk_size)
    result: list[pyodbc.Row] = cursor.fetchall()
    cursor.close()
    db.close()
    return result
    
def execute_query(query: str) -> list[pyodbc.Row]:
    db: pyodbc.Connection = pyodbc.connect(SQL_DRIVER.format(path=DB_FILE))
    cursor: pyodbc.Cursor = db.cursor().execute(query)
    result: list[pyodbc.Row] = cursor.fetchall()
    cursor.close()
    db.close()
    return result

def multiprocess_select_test() -> list[pyodbc.Row]:
    proc = []
    with futures.ProcessPoolExecutor() as pool:
        chunks = [
            pool.submit(read_chunk, 1, LAST_INDEX)
        ]
        for future in futures.as_completed(chunks):
            proc.append(future.result())

        del chunks

    for res in proc:
        for row in res:
            print(row)

    return proc
    


def multiprocess_complex_query_test() -> list[pyodbc.Row]:
    proc = []
    with futures.ProcessPoolExecutor() as pool2:
        temp = [
            pool2.submit(execute_query, queries[i % 2]) for i in range(0, 10)
        ]

        for future in futures.as_completed(temp):
            proc.append(future.result())
        
        del temp

    for res in proc:
        for row in res:
            print(row)

    return proc

def multithread_select_test() -> list[pyodbc.Row]:
    thr = []
    with futures.ThreadPoolExecutor() as pool:
        chunks = [
            pool.submit(read_chunk, 1, LAST_INDEX)
        ]
        for future in futures.as_completed(chunks):
            thr.append(future.result())

        del chunks
    
    return thr

def multithread_complex_query_test() -> list[pyodbc.Row]:
    thr = []
    with futures.ThreadPoolExecutor() as pool2:
        temp = [
            pool2.submit(execute_query, queries[i % 2]) for i in range(0, 10)
        ]

        for future in futures.as_completed(temp):
            thr.append(future.result())
        
        del temp
    
    return thr

def sequential_select_test() -> list[pyodbc.Row]:
    seq = []
    for _ in range(chunk_count):
        seq.append(read_chunk(1, LAST_INDEX))

    return seq


def sequential_complex_query_test() -> list[pyodbc.Row]:
    seq = [] 
    for i in range(0, 10):
        seq.append(execute_query(queries[i % 2]))

    return seq
