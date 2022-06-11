#Written by Ryan Helgoth and Truong Pham
import random
import sqlite3 as sql
import os
import csv

order_item_COUNT = 0

def main():
    dbSizes = ("Small", "Medium", "Large")
    for size in dbSizes:
        setUpDB(size)
    #setUpDB("Small")
    return

    
#Creates a db file based on the selected size
def setUpDB(dbSize):
    dbNames = {"Small":"A3Small.db", "Medium":"A3Medium.db", "Large":"A3Large.db"}
    dbName = dbNames[dbSize]

    path = getPath("./DBs", dbName)
    conn, csr = startConnection(path)
    dropTables(conn, csr)
    defineTables(conn, csr)

    if dbSize == "Small":
        cardinalities = {"Customers":10000, "Sellers":500, "Orders":10000, "Order_items":2000}
    elif dbSize == "Medium":
        cardinalities = {"Customers":20000, "Sellers":750, "Orders":20000, "Order_items":4000}
    elif dbSize == "Large":
        cardinalities = {"Customers":33000, "Sellers":1000, "Orders":33000, "Order_items":10000}
    
    
    populateDB(conn, csr, cardinalities)
    disconnect(conn, dbName)
    
    return

#Populates the tables of a db with data
def populateDB(connection, cursor, cardinalities):
    #Dictionary in which the key = filename and value = (TableName, (field1, ..., field n))
    fileNames = {"olist_customers_dataset.csv":("Customers", ("customer_id", "customer_postal_code")), 
                "olist_sellers_dataset.csv":("Sellers", ("seller_id", "seller_postal_code")), 
                "olist_orders_dataset.csv":("Orders", ("order_id", "customer_id")), 
                "olist_order_items_dataset.csv":("Order_items", ("order_id", "order_item_id", "product_id", "seller_id"))}

    Customers_csv_cardinality = 99441
    Sellers_csv_cardinality = 3095
    Orders_csv_cardinality = 99441
    Order_items_csv_cardinality = 112650

    CustomerID_set = set()
    sellerID_set = set()
    ValidOrders_ID  = set()   # when populating Orders, we only add rows in this set

    for fileName in fileNames:
        tableName = fileNames[fileName][0]
        tableFields = fileNames[fileName][1] 
        command = "INSERT INTO {} {} VALUES (?,?);".format(tableName, tableFields) #TODO need to add logic to handle last file which needs (?,?,?,?) instead of (?,?)

        random.seed(1)   # this seed works
        #random.seed(1000)
        
        path = getPath("./Data", fileName)
        data = open(path)
        rows_obj = csv.reader(data)
        rows_list = list(rows_obj)
        header = True #Used to skip first row (header) of csv file

        #TODO Once populating tables work, need to implement random sampling (Current implementation adds all data to db)

        if (fileName == "olist_customers_dataset.csv"):
            populate_one_table(connection, cursor, rows_list, cardinalities, "Customers", Customers_csv_cardinality, CustomerID_set)
        elif (fileName == "olist_sellers_dataset.csv"):
            populate_one_table(connection, cursor, rows_list, cardinalities, "Sellers", Sellers_csv_cardinality, sellerID_set)
        elif (fileName == "olist_orders_dataset.csv"):
            # we should filter out the order table to only have row with customer id exist in Customertable
            for i in range(1, Orders_csv_cardinality - 1):
                row = rows_list[i]
                if (row[1] in CustomerID_set):  # if this row satisfy the foreign key
                    # include that
                    ValidOrders_ID.add((row[0], row[1]))  # insert valid (orderid, customerid) 
            populate_one_table(connection, cursor, rows_list, cardinalities, "Orders", Orders_csv_cardinality, ValidOrders_ID)

        elif (fileName == "olist_order_items_dataset.csv"):
            # 
            populate_one_table(connection, cursor, rows_list, cardinalities, "Order_items", Order_items_csv_cardinality, set())

        connection.commit()
        data.close()
    
    return

"""
Select OI.order_id, OI.order_item_id, OI.product_id, OI_seller_id
from Order_items OI, Seller s, Orders o
where OI.order_id = o.order_id and OI.seller_id = s.seller_id

"""


# ---------------------------------------------
# This function will populate one table (either costumer, seller, orders, etc)
# arguments:
#   cardinalities: the dictionary 
#   table_name: one of "Customer", "Orders", etc
#   IDset: If table_name == "customer" it populate all the Customer ID in there so that Orders table only add rows that has a CUstomerID in IDset
#          
# return:
#   one table popoulated
# -----------------------------------------------
def populate_one_table(connection, cursor, rows, cardinalities, table_name, csv_cardinality, IDset):
    global order_item_COUNT

    size = cardinalities[table_name]
    size_counter = 0    # count the row added

    # -----------------------------------------------------------------------------------
    if (table_name == "Orders"):  # we just insert all the valid orders row without random
        count_row = 0
        for items in IDset:
            if (count_row == size):  # we reached the size limit
                return
            order_ID = items[0]
            CustomerID_O = items[1]
            cursor.execute(
                """
                INSERT INTO Orders(order_id, customer_id) VALUES
                    (:orderID, :CustomerID)   

                """,
                {"orderID": order_ID, "CustomerID": CustomerID_O})   
            count_row += 1 
    # ---------------------------------------------------------------------------------------------
    # Inserts Order_items
    # 
    numberofPass = 0
    if (table_name == "Order_items"):  # we just iterate from begin to end and try to add everything
        count_row = 0
        i = 1
        while True:   # iterate through all rows in csv
            if (count_row == size):
                print("inserted ", count_row, " rows to order_item ", numberofPass, " rows passed")
                return
            if (i == 112650):
                print("inserted ", count_row, " rows to order_item ", numberofPass, " rows passed")
                return

            order_item_COUNT = i
            row = rows[i]
            order_ID = row[0]             # string
            order_item_id = int(row[1])  
            product_id = row[2]          # string
            seller_id = row[3]           # string  
            try:
                cursor.execute(
                    """
                    INSERT INTO Order_items(order_id, order_item_id, product_id, seller_id) VALUES
                        (:orderID, :orderItemID, :productID, :sellerID)   
                    """,
                    {"orderID": order_ID, "orderItemID": order_item_id, "productID": product_id, "sellerID": seller_id})   
                count_row += 1
                i += 1
            except:
                numberofPass += 1 
                i += 1
                continue
        
        return
        # ----------------------------------------------------------------------------------------------

    # This is to populate Customer and Sellers
    while (size_counter < size):
        index = getRandomIndex(csv_cardinality)  # all indices except the header(index 0)

        row = rows[index]
    
        # here, I check if this row is already inserted in the table
        if (EXIST_ROW(connection, cursor, row, table_name)):
            continue  # go back to the start of the loop

        if (table_name == "Customers"):
            CustomerID = row[0]
            customer_zip = int(row[2])
            cursor.execute(
                """ 
                INSERT INTO Customers(customer_id, customer_postal_code) VALUES
                    (:ID, :PostalCode)
                """, 
                {"ID": CustomerID, "PostalCode": customer_zip})   
            # add to set
            IDset.add(CustomerID)
        elif (table_name == "Sellers"):
            # (row[0] = sellerID, row[1] = seller_postal_code)
            Seller_ID = row[0]
            Seller_postal_code = int(row[1])

            cursor.execute(
                """
                INSERT INTO Sellers(seller_id, seller_postal_code) VALUES
                    (:ID, :PostalCode)                        

                """, 
                {"ID": Seller_ID, "PostalCode": Seller_postal_code})   
            IDset.add(Seller_ID)
        size_counter += 1   

    return   


# -------------------------------------------------------
# returns a random index from 1 to csv_cardinality inclusive
# argument:
#   csv_cardinality: the number of row in the csv file of that table
# returns:
#   the randomly generated index
# ---------------------------------------------------          

def getRandomIndex(csv_cardinality):
    index = random.randint(1, csv_cardinality)
    return index

# --------------------
# return true if that row exists using sql query
# Argumenet:
#   row: the row that we are checking
#   table_name: name of the table, "Customer", "Sellers", etc
# return
#   true if that row is already inserted, false otherwise
# ------------------------
def EXIST_ROW(connection, cursor, row, table_name):
    flag = False

    if (table_name == "Customers"):
        cursor.execute("""SELECT 1 from Customers t WHERE t.customer_id = :cid LIMIT 1""", 
            {"cid": row[0]})  # check for matching customerid
    elif (table_name == "Sellers"):
        cursor.execute("""SELECT 1 from Sellers t WHERE t.seller_id = :sid LIMIT 1""", 
            {"sid": row[0]})  # check for matching sellerid        
    elif (table_name == "Orders"):
        cursor.execute("""SELECT 1 from Orders  t WHERE t.order_id = :oid_ LIMIT 1""", 
            {"oid_": row[0]})  # check for matching orderid
    elif (table_name == "Order_items"):
        cursor.execute("""SELECT 1 from Order_items  t WHERE t.order_item_id = :oid_itemid
            and t.order_id = :oid_ and t.seller_id = :sid and t.product_id = :pid LIMIT 1""", 
            {"oid_itemid": row[1], "oid_":row[0], "sid": row[3], "pid": row[2]})  # check for matching orderid    
    flag = cursor.fetchone() is not None  


    return flag

# PRIMARY KEY("order_id","order_item_id","product_id","seller_id"),

def getPath(folder, fileName):
    return os.path.join(folder, fileName)   
    

def startConnection(path):
    connection = sql.connect(path)
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON;")   #TODO check if this is needed
    connection.commit()
    return connection, cursor



def disconnect(connection, dbName):
    connection.close()
    print("Connection to {} has been closed.".format(dbName))
    return



def dropTables(connection, cursor):
    dropCustomers = "DROP TABLE IF EXISTS Customers;"
    dropSellers = "DROP TABLE IF EXISTS Sellers;"
    dropOrders = "DROP TABLE IF EXISTS Orders;"
    dropOrderitems = "DROP TABLE IF EXISTS Order_items"

    cursor.execute(dropOrderitems)
    cursor.execute(dropOrders)
    cursor.execute(dropCustomers)
    cursor.execute(dropSellers)

    connection.commit()
    return

def defineTables(connection, cursor):
    customersTab =   """
    CREATE TABLE "Customers" ( 
	"customer_id"	TEXT, 			        
	"customer_postal_code"	INTEGER, 	
	PRIMARY KEY("customer_id")
    );

    """ 
    sellersTab = """
    CREATE TABLE "Sellers" (
	"seller_id"	TEXT, 				
	"seller_postal_code"	INTEGER, 	        
	PRIMARY KEY("seller_id")
    );
    """
    ordersTab = """
    CREATE TABLE "Orders" (
	"order_id"	TEXT,				
	"customer_id"	TEXT,			        
	PRIMARY KEY("order_id")
    FOREIGN KEY("customer_id") REFERENCES "Customers"("customer_id")
    );    """

    orderItemsTab = """
    CREATE TABLE "Order_items" (
	"order_id"	TEXT,				
	"order_item_id"	INTEGER,		        
	"product_id"		TEXT,			
	"seller_id"	TEXT,				
	PRIMARY KEY("order_id","order_item_id","product_id","seller_id"),
	FOREIGN KEY("seller_id") REFERENCES "Sellers"("seller_id")
    FOREIGN KEY("order_id") REFERENCES "Orders"("order_id")
    );
    """


    cursor.execute(customersTab)
    cursor.execute(sellersTab)
    cursor.execute(ordersTab)
    cursor.execute(orderItemsTab)
    connection.commit()
    return





if __name__ == '__main__':
    main()
"""
Select OI.order_id, OI.order_item_id, OI.product_id, OI_seller_id
from Order_items OI, Seller s, Orders o
where OI.order_id = o.order_id and OI.seller_id = s.seller_id



        # DELETE THIS BELOW
        # elif (table_name == "Orders"):
        #     # order_ID = row[0]     # string 
        #     # CustomerID_O = row[1]  # string, i know its unlikely but, 
         


        #     # cursor.execute(
        #     #     """
        #     #     INSERT INTO Orders(order_id, customer_id) VALUES
        #     #         (:orderID, :CustomerID)   

        #     #     """,
        #     #     {"orderID": order_ID, "CustomerID": CustomerID_O})   
        #     break   
               
        # elif (table_name == "Order_items"):
        #     # order_ID = row[0]             # string
        #     # order_item_id = int(row[1])  
        #     # product_id = row[2]          # string
        #     # seller_id = row[3]           # string
        #     # try:
        #     #     cursor.execute(
        #     #         """
        #     #         INSERT INTO Order_items(order_id, order_item_id, product_id, seller_id) VALUES
        #     #             (:orderID, :orderItemID, :productID, :sellerID)   
        #     #         """,
        #     #         {"orderID": order_ID, "orderItemID": order_item_id, "productID": product_id, "sellerID": seller_id})   
        #     # except:
        #     #     continue
        #     break