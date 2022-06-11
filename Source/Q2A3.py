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
        print("Uniformed: ", uninformedTime) #TODO remove this line
        disconnect(conn, dbName)
        times["Uninformed"].append(uninformedTime)


        conn, csr = startConnection(path, dbName)
        selfOptimizedTime = timeSelfOptimized(csr, conn)
        print("selfOptimizedTime: ", selfOptimizedTime) #TODO remove this line
        disconnect(conn, dbName)
        times["Self-Optimized"].append(selfOptimizedTime)

        conn, csr = startConnection(path, dbName)
        userOptimizedTime = timeUserOptimized(csr, conn)
        disconnect(conn, dbName)
        times["User-Optimized"].append(userOptimizedTime)

        print("UserOptimized: ", userOptimizedTime) 


    plotResults(dbNames, times)
    return

# ---------------------------------------
# time UserOptimized scnerio
# ------------------------------------------
def timeUserOptimized(csr, conn):
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = ON;") 
    # create indices here
    csr.execute("CREATE INDEX Orders_idx_forview ON Orders(order_id);")   # index on OrderID to try to improve view searching
    
    conn.commit()
    createView(csr, conn)

    csr.execute("CREATE INDEX OrdersIdx1 ON Orders (customer_id);")   # index on Orders(customer_id) to improve join time
    csr.execute("CREATE INDEX CustomersIdx1 ON Customers (Customer_postal_code);")  # index on Customers(Customer_postal_code)

    conn.commit()
    avgtime = query2time(csr)
    csr.execute(""" drop view OrderSize; """)
    
    csr.execute("DROP INDEX OrdersIdx1;")
    csr.execute("DROP INDEX CustomersIdx1;")
    csr.execute("DROP INDEX Orders_idx_forview; ")
    conn.commit()
    return avgtime



# ---------------------------------------
# time selfOptimized scnerio
# ------------------------------------------
def timeSelfOptimized(csr, conn):
    csr.execute("PRAGMA automatic_index = TRUE;")
    csr.execute("PRAGMA foreign_keys = ON;")  
    conn.commit()
     
    createView(csr, conn)

    avgtime = query2time(csr)
    csr.execute(""" drop view OrderSize;""")

    conn.commit()
    return avgtime

# ---------------------------------------
# time uniformed scnerio
# ------------------------------------------
def timeUninformed(csr, conn):
    # extend Q1 with the information of the average number of items each order had.
    csr.execute("PRAGMA automatic_index = FALSE;")
    csr.execute("PRAGMA foreign_keys = OFF;")  
    conn.commit()
    undefKeys(csr, conn)



    # Create the view
    createView(csr, conn)

    avgtime = query2time(csr)
    csr.execute(""" drop view OrderSize;""")

    conn.commit()
    defKeys(csr, conn)

    return avgtime

# ----------------------------------
# creates the view in the db
# Argument:
#       csr
#       conn
# Returns:
#       none
# -----------------------------------
def createView(csr, conn):
    viewQuery = """ 
    Create view OrderSize as 
    SELECT o.order_id as oid, COALESCE(ordersize.size, 0) as size
    from Orders o left join
    (SELECT oi.order_id, count(*) as size
    from Order_items oi
    group by oi.order_id) as ordersize
    on o.order_id = ordersize.order_id
    """
    csr.execute(viewQuery)
    conn.commit()
    return

# ------------------------------------------
# Execute the query2 50 times and collect the times taken
# Argument:
#       csr
# returns:
#       the average time the query took
# ------------------------------------------
def query2time(csr):
    # the average number number of items each order had
    
    queryStr = """

    SELECT count(*) as countoid, cast(sum(os.size) as real) / cast(count(*) as real) as average 
    From Customers c, Orders o, OrderSize os
    Where c.customer_postal_code = :postalcode and c.customer_id = o.customer_id and o.order_id = os.oid

    """
    time_array = []
    for i in range(50):
        randoPostalcode = getRandCustomerPostalCode(csr)
        startTime = time.time()
        csr.execute(queryStr, {"postalcode": randoPostalcode})
        endTime = time.time()
        time_array.append(endTime - startTime)
        

        #row = csr.fetchall()
        #printrow(row)

    avgtime = cmputavgtime(time_array)
    return avgtime
# ------------------------------------------
# cmput the average time given an array of times
# Argument:
#       time_array = an array of times
# Returns:
#       the average time
# ------------------------------------------
def cmputavgtime(time_array):
    n = len(time_array)
    sum_ = 0 
    for i in range(n):
        sum_ += time_array[i]

    return sum_ / n
# --------------------------------------
# get random postalcode for query 1, written by Ryan Helgoth
# Arguments:
#       csr
# returns:
#       randon postalcode from Customer table
# -------------------------------------------
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

def startConnection(path, dbName):
    connection = sql.connect(path)
    cursor = connection.cursor()
    #cursor.execute("PRAGMA foreign_keys=ON;")   #TODO check if i need to remove this
    connection.commit()
    print("Successfully connected to {}.".format(dbName))
    return connection, cursor


def disconnect(connection, dbName):
    connection.close()
    print("Connection to {} has been closed.".format(dbName))
    return


def getPath(folder, fileName):
    return os.path.join(folder, fileName)  


def plotResults(dbTimes):
	return



def undefKeys(csr, conn):
    customersTab =   """
    CREATE TABLE "NoKeysCustomers" ( 
    "customer_id"   TEXT,                   
    "customer_postal_code"  INTEGER
    );

    """ 
    sellersTab = """
    CREATE TABLE "NoKeysSellers" (
    "seller_id" TEXT,               
    "seller_postal_code"    INTEGER
    );
    """
    ordersTab = """
    CREATE TABLE "NoKeysOrders" (
    "order_id"  TEXT,               
    "customer_id"   TEXT
    );    """

    orderItemsTab = """
    CREATE TABLE "NoKeysOrder_items" (
    "order_id"  TEXT,               
    "order_item_id" INTEGER,                
    "product_id"        TEXT,           
    "seller_id" TEXT
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
# ----------------------------------------
# plot the chart: written by Ryan Helgoth
# Arguments:
#       xlabels: ["A3Small.db", "A3Medium.db", "A3Large.db"]
#       times: dictionary of {scnerio: avgtime}
# ---------------------------------------------
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
    ax.set_title("Query 2 (runtime in ms)")
    ax.legend()

    path = getPath("./Charts", "Q2A3chart.png") #TODO change path before submitting (dbs should be in same folder as python scripts)
    plt.savefig(path)
    print('Chart saved to file {}'.format(path))
    plt.close()
    return


def printrow(row):
    for i in range(len(row)):
        print(row[i])
    return

if __name__ == "__main__":
	main()


"""
SELECT oi.order_id, count(*) as count1
FROM Order_items oi
group by oi.order_id

this is the query to create the view

"""

"""

dbTimes = {"A3Small.db":[], "A3Medium.db":[], "A3Large.db":[]}
for dbName in dbTimes:
    path = getPath("./DBs", dbName) #TODO change path before submitting (dbs should be in same folder as python scripts)
    
    conn, csr = startConnection(path, dbName)
    uninformedTime = timeUninformed(csr, conn)
    print("Uniformed: ", uninformedTime) #TODO remove this line
    disconnect(conn, dbName)
    dbTimes[dbName].append(uninformedTime)
    
    # conn, csr = startConnection(path, dbName)
    # selfOptimizedTime = timeSelfOptimized(csr, conn)
    # print("selfOptimizedTime: ", selfOptimizedTime) #TODO remove this line
    # disconnect(conn, dbName)
    # dbTimes[dbName].append(selfOptimizedTime)
    
    # conn, csr = startConnection(path, dbName)
    # userOptimizedTime = timeUserOptimized(csr, conn)
    # disconnect(conn, dbName)
    # dbTimes[dbName].append(userOptimizedTime) 
    

plotResults(dbTimes)
return




"""
# q1 query test:
'''
select customer_postal_code, count(*)
from Customers c, Orders o
where c.customer_postal_code = 62790
and C.customer_id = o.customer_id

'''
# create view query
'''
    Create view OrderSize as
    SELECT oi.order_id, count(*) as size
    FROM Order_items oi
    group by oi.order_id



'''
'''
SELECT q1.order_id, customer_postal_code, sum(orderSize.Size)/ count(*)
FROM orderSize,

(select customer_postal_code, order_id
from Customers c, Orders o
where c.customer_postal_code = 88385
and C.customer_id = o.customer_id) as q1
where q1.order_id = orderSize.order_id
'''
'''
this returns orderid postalcode(they are all the same), and #of item

SELECT q1.order_id, customer_postal_code, Size
FROM 
(select customer_postal_code, order_id
from Customers c, Orders o
where c.customer_postal_code = 13054
and C.customer_id = o.customer_id) as q1 
LEFT JOIN orderSize
on orderSize.order_id = q1.order_id

'''
'''
this is view definition of OrderSize, size(replaced null with 0)

Create view OrderSize as 
SELECT o.order_id, COALESCE(ordersize.size, 0) as size
from Orders o left join
(SELECT oi.order_id, count(*) as size
from Order_items oi
group by oi.order_id) as ordersize
on o.order_id = ordersize.order_id

'''
'''
This is the final query

    SELECT customer_postal_code , count(*) as countoid, cast(sum(OrderSize.size) as real)/ cast(count(*) as real) as average
    FROM
    (select customer_postal_code, order_id
    from Customers c, Orders o
    where c.customer_postal_code = :postalcode
    and C.customer_id = o.customer_id) as q1 
    left join OrderSize
    on OrderSize.oid = q1.order_id


'''


'''
better query

    SELECT count(*) as countoid, cast(sum(os.size) as real) / cast(count(*) as real) as average 
    From Customers c, Orders o, OrderSize os
    Where c.customer_postal_code = :postalcode and c.customer_id = o.customer_id and o.order_id = os.oid



'''
