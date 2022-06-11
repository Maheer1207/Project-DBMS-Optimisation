#Written by Ryan Helgoth

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

#Returns time in ms of uniformed scenario
def timeUninformed(csr, conn):
    #[(4, 0, 0, 'SCAN TABLE Customers AS C'), (8, 0, 0, 'SCAN TABLE Orders AS O')]
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = OFF;")  
    conn.commit()
    
    undefKeys(csr, conn)
    timeTaken = timeQuery1(csr)
    defKeys(csr, conn)

    return timeTaken    

#Returns time in ms of self-optimized scenario
def timeSelfOptimized(csr, conn):
    #[(5, 0, 0, 'SCAN TABLE Orders AS O'), (7, 0, 0, 'SEARCH TABLE Customers AS C USING INDEX sqlite_autoindex_Customers_1 (customer_id=?)')]
    csr.execute("PRAGMA automatic_index = TRUE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
    conn.commit()

    timeTaken = timeQuery1(csr)
    return timeTaken    

#Returns time in ms of user-optimized scenario
def timeUserOptimized(csr, conn):
    #[(5, 0, 0, 'SEARCH TABLE Customers AS C USING INDEX CustomersIdx1 (customer_postal_code=?)'), (12, 0, 0, 'SEARCH TABLE Orders AS O USING COVERING INDEX OrdersIdx1 (customer_id=?)')]
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
    csr.execute("CREATE INDEX OrdersIdx1 ON Orders (customer_id);")
    csr.execute("CREATE INDEX CustomersIdx1 ON Customers (customer_postal_code);")
    conn.commit()

    timeTaken = timeQuery1(csr)
    csr.execute("DROP INDEX OrdersIdx1;")
    csr.execute("DROP INDEX CustomersIdx1;")
    conn.commit()
    return timeTaken    
    
#Returns time in ms of query 1 run 50 times
def timeQuery1(csr):
    totalTime = 0
    for i in range(50):
        postalCode = getRandCustomerPostalCode(csr)
        query = '''
        SELECT COUNT(*) 
        FROM Customers AS C, Orders AS O
        WHERE C.customer_postal_code = :postalCode
        AND C.customer_id = O.customer_id;
        '''
        startTime = time.time()
        csr.execute(query, {"postalCode": postalCode}) 
        endTime = time.time()
        totalTime += (endTime - startTime)
    return totalTime * 1000 #miliseconds

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
    ax.set_title("Query 1 (runtime in ms)")
    ax.legend()

    path = getPath("./Charts", "Q1A3chart.png") #TODO change path before submitting (dbs should be in same folder as python scripts)
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