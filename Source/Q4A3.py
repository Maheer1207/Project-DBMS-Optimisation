import sqlite3 as sql
import os
import time
import matplotlib.pyplot as plt


#Function to call all the scenarios and sent the result to plot the bar chart
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

#Returns time in ms of the uninformed scenario 
def timeUninformed(csr, conn):

    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = OFF;")
  
    conn.commit()

    undefKeys(csr, conn)
    timeTaken = timeQuery4(csr)
    defKeys(csr, conn)

    return timeTaken    

#Returns time in ms of the self-optimized scenario 
def timeSelfOptimized(csr, conn):

    csr.execute("PRAGMA automatic_index = TRUE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
      
    conn.commit()

    timeTaken = timeQuery4(csr)

    return timeTaken


#Returns time in ms of the user-optimized scenario 
def timeUserOptimized(csr, conn):
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = ON;")
    csr.execute("CREATE INDEX OrdersIdx4 ON Order_items (order_id);")
    csr.execute("CREATE INDEX SellersIdx4 ON Sellers (seller_id);")
    conn.commit()

    timeTaken = timeQuery4(csr)
    csr.execute("DROP INDEX OrdersIdx4;")
    csr.execute("DROP INDEX SellersIdx4;")
    conn.commit()
    return timeTaken
  
    
#Runs the query 50 times and counts the query processing time 
def timeQuery4(csr):
    #https://stackoverflow.com/a/20194287
    totalTime = 0
    for i in range(50):
        orderId = getRandOrderId(csr)
        query = '''
        SELECT count(DISTINCT s.seller_postal_code)  
        FROM Order_items o_i, Sellers s 
        WHERE o_i.order_id = :orderId
        AND o_i.seller_id = s.seller_id
        '''
        startTime = time.time()
        csr.execute(query, {"orderId": orderId}) 
        endTime = time.time()
        totalTime += (endTime - startTime)
    return totalTime * 1000 #miliseconds


#Creates new relations with out keys, fills the  new relations with the original values, and renames it to the original relation's name 
def undefKeys(csr, conn):


    #Creates a new relation "NoKeysCustomers", which doesn't have any keys  
    customersTab =   """
    CREATE TABLE "NoKeysCustomers" ( 
	"customer_id"	TEXT, 			        
	"customer_postal_code"	INTEGER
    );""" 
	
    #Creates a new relation "NoKeysSellers", which doesn't have any keys  
    sellersTab = """
    CREATE TABLE "NoKeysSellers" (
	"seller_id"	TEXT, 				
	"seller_postal_code"	INTEGER
    );"""

    #Creates a new relation "NoKeysSellers", which doesn't have any keys  
    ordersTab = """
    CREATE TABLE "NoKeysOrders" (
	"order_id"	TEXT,				
	"customer_id"	TEXT
    );"""



    #Creates a new relation "NoKeysSellers", which doesn't have any keys  

    orderItemsTab = """
    CREATE TABLE "NoKeysOrder_items" (
	"order_id"	TEXT,				
	"order_item_id"	INTEGER,		        
	"product_id"		TEXT,			
	"seller_id"	TEXT
    );"""

    csr.execute(customersTab)
    csr.execute(sellersTab)
    csr.execute(ordersTab)
    csr.execute(orderItemsTab)

    #Insert values in the new relations from the corresponding old relations 
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


    #Rename the relations  
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
    
    

#Drops the relations without keys, and renames the original relations to orginal names 
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

    



#Generates the random order id, it will be given so    
def getRandOrderId(csr):
    #https://www.sqlitetutorial.net/sqlite-functions/sqlite-random/
    query = '''
            SELECT o.order_id
            FROM Orders AS o 
            ORDER BY RANDOM() 
            LIMIT 1;
            '''
    csr.execute(query)
    resultTuple = csr.fetchone()
    postalCode = resultTuple[0]

    return postalCode #int


#Plots the results to a bar chart 
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
    ax.set_title("Query 4 (runtime in ms)")
    ax.legend()

    path = getPath("./Charts", "Q4A3chart.png") #TODO change path before submitting (dbs should be in same folder as python scripts)
    plt.savefig(path)
    print('Chart saved to file {}'.format(path))
    plt.close()

    return

#Generates the path, taking the folder's name and the file's name 
def getPath(folder, fileName):
    return os.path.join(folder, fileName)   

#Creates the connection 
def startConnection(path, dbName):
    connection = sql.connect(path)
    cursor = connection.cursor()
    connection.commit()
    print("Successfully connected to {}.".format(dbName))
    return connection, cursor

#Disconnects the connection 
def disconnect(connection, dbName):
    connection.close()
    print("Connection to {} has been closed.".format(dbName))
    return

if __name__ == '__main__':
    main()
