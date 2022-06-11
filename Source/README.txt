Group Number: 10
Group Members:
	-Name: Ryan Helgoth, CCID: helgoth
	-Name: Truong-Giang Pham, CCID: truonggi
	-Name: Mohammad Mohaiminul Islam Maheer, CCID: mmaheer
	-Name: Sukhyun Byun, CCID: sukhyun

Resources used:
	-https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_stacked.html
	-https://www.sqlitetutorial.net/sqlite-functions/sqlite-random/

Collaboration:
	-We declare that we did not collaborate with anyone outside our own group in this assignment
	
Index Reasoning:
	-Query 1 (Ryan Helgoth):
		-I executed the following Query:
			SELECT COUNT(*) 
        		FROM Customers AS C, Orders AS O
        		WHERE C.customer_postal_code = :postalCode
        		AND C.customer_id = O.customer_id;

		-The first index I created was on "customer_id" from the Orders table. 
		I chose this index as it is a covering index as confirmed by executing 
		"EXPLAIN QUERY PLAN". This offers a large preformance increase as it 
		signifigantly reduces the total amount of disk acsesses the query makes.
		Sqlite's auto indexing (as seen in the self-optimized scenario)
		chooses to instead scan the Orders table and use an index on "customer_id"
		from the Customers table. This results in much worse preformance due to
		the fact that Orders must be scaned and the index used is non-covering.
		
		-The second index I created was on "customer_postal_code" from the 
		Customers table. This index offers a smaller but still signifigant increase 
		in preformance. I chose this index because we only care about customers 
		with the given postal code, so finding it quickly means that we have to
		look through less customers/tuples.
		
	-Query 2(Truong-Giang Pham):
        -I created the following view using the definition:
                Create view OrderSize as 
                SELECT o.order_id as oid, COALESCE(ordersize.size, 0) as size
                from Orders o left join
                (SELECT oi.order_id, count(*) as size
                from Order_items oi
                group by oi.order_id) as ordersize
                on o.order_id = ordersize.order_id
	     Initially this view defition should've done the job:
                SELECT oi.order_id, count(*) as size
                from Order_items oi
                group by oi.order_id
         However I later found that many order_id in Orders table are not in Order_items table,
         so I had to join the query above with Orders to make my OrderSize view contain ALL the order_id.
         So my final view defition take into account of ALL order_id, and order_id that does not exist in Order_items 
         are set to 0 as size

        -I executed this query for q2:
            SELECT count(*) as countoid, cast(sum(os.size) as real) / cast(count(*) as real) as average 
            From Customers c, Orders o, OrderSize os
            Where c.customer_postal_code = :postalcode and c.customer_id = o.customer_id and o.order_id = os.oid
        
        - The first index I created was on the primary key Orders(order_id) because I learned that 
        we cannot create index on a view, but view uses the index of the underlying table. 
        This index was made in attempt to improve the time when I'm joining with the view.
        However, it did not increase the time much although EXPLAIN QUERY PLAN did showed that sql using this index
        when joining.

        -The index that significantly increased the time was the two index: index on Orders(customer_id) 
        and a covering index on  Customers(postal_Code).
        These index made the query signicantly faster because the cardinality of Customers and Orders are the greatest.
        Therefore, an improvement on Customers table's search on postal_code, and the improvement on the join by customer_id(for Orders)
        made it better.

        -On a side note, I thought an index on the primary key Customers(customer_id) would make it faster since it covers the join between
        (Customers JOIN  Orders on customer_id) together with my index on Orders(customer_id). Howver, nothing changed.
        
	-Query 3 (Sukhyun Byun):

		- I executed the following Query:
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
		
		- This query is very similar in nature to query 2 except rather than using a view, the subquery is embedded
		  in the sql and directly defines the table OrderSize. 

		- The first index I created was order_id in Orders.
		  This is a covering index as confirmed by executing "EXPLAIN QUERY PLAN." 
		  While it did not improve the time significantly compared to other two indices used, it is worth noting that this 
		  index was used when left joining the tables, and is a covering index (includes all the columns).

		- The second index I created was customer_id in Orders. 
		  Since we are only concerned with only the customers who have ordered, we can access less customers (tuples) or disregard customers with no orders.
		  This improves our performance. 
		  Also note that the cardinalities of Customers and Orders are significantly higher than other two tables in each database;
		  intuitively I thought that improving our search time in Customers and Orders would overall result in a better performance. 

		- The third index I created was customer_postal_code in Customers.
		  Similar to the first index, the query looks for customers with a specific customer_postal_code, meaning all other customers
		  who do not have that specific customer_postal_code are irrelevant. By narrowing down our search, our query becomes significantly
		  faster.
		  
	-Query 4(Mohammad Mohaiminul Islam Maheer):

		- I executed the following Query:
			SELECT count(DISTINCT s.seller_postal_code)
			FROM Order_items o_i, Sellers s
			WHERE o_i.order_id = :orderId
			AND o_i.seller_id = s.seller_id;
      
		- For query 4, in the user optimized scenario, my first index was on  
		  "order_id" from the relaltion Order_Items. The reason behind choosing  
		  "order_id" is that, in the query we were asked to find unique postal  
		  codes of the sellers that fulfilled the randomly chosen "order_id" from 
		  the relations Orders. Hence, to look if the order of the randomly  
		  chosen "order_id" have been completed, we need to look for the  
		  "order_id" in the Order_items. Using an user-defined  index on "order_id", the 
		  query rather than going for the uniformed or auto index, the query becomes  
		  more efficient in every cases. Although, the difference in the bar chart between  
		  the "Self-Optimised" and "User-Optimised" was not that significant. One of the 
		  reason of such minimal difference could be the use of a pretty optimal index 
		  while auto indexing as well. Moreover, another reason behind thinking 
		  “order_id” would the better all others is, to look for the records in the 
		  Order_items for the completion of the order we will look on the basis of 
		  “order_id”. Thus, use of any other keys index wouldn’t be that much helpful.

		- The second index was on the “seller_id” in the Sellers relation, as for each of 
		  the completed orders, we need to bring up the unique postal code of the of the 
		  sellers who completed the order. So, we need to look in the Sellers relation for  
		  the corresponding “seller_id” related to the order completed data in the 
		  Order_item relation. As, we will be looking for the “seller_postal_code“ on the  
		  basis of “seller_id” from the Seller table, an index on the “seller_id” would be the 
		  most optimal one. However, if we use any other key for searching the 
		  “seller_postal_code” of any corresponding “seller_id” it won’t be that optimal.
