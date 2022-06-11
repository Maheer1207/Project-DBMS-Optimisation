import sqlite3 as sql
import os
import time
import matplotlib.pyplot as plt

def main():
    dbNames = ("A3Small.db", "A3Medium.db", "A3Large.db")
    times = {"Uninformed":[], "Self-Optimized":[], "User-Optimized":[]}
    for dbName in dbNames:
        path = getPath("./DBs", dbName) #TODO change path before submitting (dbs should be in same folder as python scripts)
        
        conn, csr = startConnection(path, dbName)
        uninformedTime = timeUninformed(csr, conn)
        print("uninformedTime: ", uninformedTime) 
        disconnect(conn, dbName)
        times["Uninformed"].append(uninformedTime)
        
        conn, csr = startConnection(path, dbName)
        selfOptimizedTime = timeSelfOptimized(csr, conn)
        print("selfOptimizedTime: ", selfOptimizedTime) 
        disconnect(conn, dbName)
        times["Self-Optimized"].append(selfOptimizedTime)
        
        conn, csr = startConnection(path, dbName)
        userOptimizedTime = timeUserOptimized(csr, conn)
        print("userOptimizedTime: ", userOptimizedTime) 
        disconnect(conn, dbName)
        times["User-Optimized"].append(userOptimizedTime)
        
    plotResults(dbNames, times)
    return

#Returns time in millisecond of uniformed scenario
def timeUninformed(csr, conn):
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = OFF;")  
    conn.commit()
    
    undefKeys(csr, conn)
    avgTime = avgTimeQuery3(csr)
    defKeys(csr, conn)

    return avgTime

#Returns time in millisecond of self-optimized scenario
def timeSelfOptimized(csr, conn):
    csr.execute("PRAGMA automatic_index = TRUE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
    conn.commit()

    avgTime = avgTimeQuery3(csr)
    return avgTime    

#Returns time in milisecond of user-optimized scenario
def timeUserOptimized(csr, conn):
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
    csr.execute("CREATE INDEX OrdersIdx1 ON Orders (order_id);")
    csr.execute("CREATE INDEX OrdersIdx2 ON Orders (customer_id);")
    csr.execute("CREATE INDEX CustomersIdx1 ON Customers (customer_postal_code);")
    conn.commit()

    avgTime = avgTimeQuery3(csr)
    csr.execute("DROP INDEX OrdersIdx1;")
    csr.execute("DROP INDEX OrdersIdx2;")
    csr.execute("DROP INDEX CustomersIdx1;")
    conn.commit()
    return avgTime    
    
#Returns time in ms of query 3 run 50 times
def avgTimeQuery3(csr):
    query = """
        SELECT COUNT(*) AS numOrders, CAST(SUM(OrderSize.size) AS REAL) / CAST(COUNT(*) AS REAL) AS Average
        FROM Customers C, Orders O,
        (SELECT O.order_id, COALESCE(checkOrder.size, 0) as size
        FROM   Orders O
        LEFT JOIN
        (SELECT Oi.order_id, COUNT(*) AS size 
        FROM    Order_items Oi
        GROUP BY Oi.order_id) as checkOrder
        ON      O.order_id = checkOrder.order_id) as OrderSize
        WHERE   C.customer_postal_code = :postalCode and C.customer_id = O.customer_id and O.order_id = OrderSize.order_id;
        """
    time_arr = []
    for i in range(50):
        postalCode = getRandCustomerPostalCode(csr)
        startTime = time.time()
        csr.execute(query, {"postalCode": postalCode})
        endTime = time.time()
        time_arr.append(endTime - startTime)
    
    avgTime = cmputeAvg(time_arr)
    return avgTime

#Compute the average time of all the times 
def cmputeAvg(time_arr):
    n = len(time_arr)
    totalSum = 0
    for i in range(n):
        totalSum += time_arr[i]
    avgTime = totalSum / n

    return avgTime

#Undefines primary and foreign keys of tables in the db
def undefKeys(csr, conn):
    customersTab =   """
    CREATE TABLE "NoKeysCustomers" ( 
	"customer_id"	TEXT, 			        
	"customer_postal_code"	INTEGER
    );
    """ 

    sellersTab = """
    CREATE TABLE "NoKeysSellers" (
	"seller_id"	TEXT, 				
	"seller_postal_code"	INTEGER
    );
    """

    ordersTab = """
    CREATE TABLE "NoKeysOrders" (
	"order_id"	TEXT,				
	"customer_id"	TEXT
    );    """

    orderItemsTab = """
    CREATE TABLE "NoKeysOrder_items" (
	"order_id"	TEXT,				
	"order_item_id"	INTEGER,		        
	"product_id"		TEXT,			
	"seller_id"	TEXT
    );
    """

    csr.execute(customersTab)
    csr.execute(sellersTab)
    csr.execute(ordersTab)
    csr.execute(orderItemsTab)
    

    csr.execute('''
                INSERT INTO NoKeysCustomers 
                SELECT customer_id, customer_postal_code 
                FROM Customers;
                ''')
    csr.execute('''
                INSERT INTO NoKeysSellers 
                SELECT seller_id, seller_postal_code 
                FROM Sellers;
                ''')
    csr.execute('''
                INSERT INTO NoKeysOrders
                SELECT order_id, customer_id
                FROM Orders;
                ''')
    csr.execute('''
                INSERT INTO NoKeysOrder_items 
                SELECT order_id, order_item_id, product_id, seller_id
                FROM Order_items;
                ''')

    csr.execute("ALTER TABLE Customers RENAME TO CustomersOriginal;")
    csr.execute("ALTER TABLE NoKeysCustomers RENAME TO Customers;")

    csr.execute("ALTER TABLE Sellers RENAME TO SellersOriginal;")
    csr.execute("ALTER TABLE NoKeysSellers RENAME TO Sellers;")

    csr.execute("ALTER TABLE Orders RENAME TO OrdersOriginal;")
    csr.execute("ALTER TABLE NoKeysOrders RENAME TO Orders;")

    csr.execute("ALTER TABLE Order_items RENAME TO Order_itemsOriginal;")
    csr.execute("ALTER TABLE NoKeysOrder_items RENAME TO Order_items;")

    conn.commit()
    return
    
    
#Redefines primary and foreign keys for tables in the db
def defKeys(csr, conn):

    csr.execute("DROP TABLE Customers;")
    csr.execute("ALTER TABLE CustomersOriginal RENAME TO Customers;")

    csr.execute("DROP TABLE Sellers;")
    csr.execute("ALTER TABLE SellersOriginal RENAME TO Sellers;")

    csr.execute("DROP TABLE Orders;")
    csr.execute("ALTER TABLE OrdersOriginal RENAME TO Orders;")

    csr.execute("DROP TABLE Order_items;")
    csr.execute("ALTER TABLE Order_itemsOriginal RENAME TO Order_items;")

    conn.commit()
    return


#Returns random customer postal code from db   
def getRandCustomerPostalCode(csr):
    #https://www.sqlitetutorial.net/sqlite-functions/sqlite-random/
    query = '''
            SELECT C.customer_postal_code
            FROM Customers AS C 
            ORDER BY RANDOM() 
            LIMIT 1;
            '''
    csr.execute(query)
    resultTuple = csr.fetchone()
    postalCode = resultTuple[0]

    return postalCode #int

#Plots the times taken for each scenario for each db
def plotResults(xLabels, times):
    #https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_stacked.html
    fig, ax = plt.subplots() 
    
    bottomValues = [0,0,0]
    for scenario in times:
        ax.bar(xLabels, times[scenario], 0.40, bottom = bottomValues, label = scenario)

        #Sets y-coordinents where the next times will be plotted
        for i in range(len(bottomValues)):
            bottomValues[i] = bottomValues[i] + times[scenario][i]

            
    ax.set_ylabel("Runtime (ms)")
    ax.set_title("Query 3 (runtime in ms)")
    ax.legend()

    path = getPath("./Charts", "Q3A3chart.png") #TODO change path before submitting (dbs should be in same folder as python scripts)
    plt.savefig(path)
    print('Chart saved to file {}'.format(path))
    plt.close()
    return

#Returns a file's path based on the folder and filename
def getPath(folder, fileName):
    return os.path.join(folder, fileName)   

#Starts a connection to a db
def startConnection(path, dbName):
    connection = sql.connect(path)
    cursor = connection.cursor()
    connection.commit()
    print("Successfully connected to {}.".format(dbName))
    return connection, cursor

#Dissconnects from a db
def disconnect(connection, dbName):
    connection.close()
    print("Connection to {} has been closed.".format(dbName))
    return

if __name__ == '__main__':
    main()