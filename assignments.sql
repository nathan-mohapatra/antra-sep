USE WideWorldImporters;

-- 1. List of Persons’ full name, all their fax and phone numbers, as well as the phone number and 
-- fax of the company they are working for (if any).
SELECT P.FullName, P.PhoneNumber AS PersonalPhone, P.FaxNumber AS PersonalFax, 
	COALESCE(C.PhoneNumber, S.PhoneNumber) AS CompanyPhone, 
	COALESCE(C.FaxNumber, S.FaxNumber) AS CompanyFax
FROM Application.People P
	LEFT JOIN Sales.Customers C 
	ON (P.PersonID = C.PrimaryContactPersonID) OR (P.PersonID = C.AlternateContactPersonID)
	LEFT JOIN Purchasing.Suppliers S 
	ON (P.PersonID = S.PrimaryContactPersonID) OR (P.PersonID = S.AlternateContactPersonID)
WHERE P.FullName != 'Data Conversion Only';

-- 2. If the customer's primary contact person has the same phone number as the customer’s phone 
-- number, list the customer companies.
SELECT C.CustomerName
FROM Application.People P 
	INNER JOIN Sales.Customers C
	ON (P.PersonID = C.PrimaryContactPersonID) AND (P.PhoneNumber = C.PhoneNumber);

-- 3. List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
SELECT DISTINCT C.CustomerName
FROM Sales.Customers C
	INNER JOIN Sales.CustomerTransactions T
	ON C.CustomerID = T.CustomerID
WHERE T.TransactionDate < '2016-01-01' AND T.CustomerID NOT IN (
	SELECT CustomerID
	FROM Sales.CustomerTransactions
	WHERE TransactionDate > '2016-01-01'
);

-- 4. List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.
SELECT S.StockItemName, SUM(P.OrderedOuters) AS TotalQuantity
FROM Purchasing.PurchaseOrderLines P
	INNER JOIN Warehouse.StockItems S
	ON P.StockItemID = S.StockItemID
WHERE YEAR(P.LastReceiptDate) = '2013'
GROUP BY S.StockItemName
ORDER BY TotalQuantity DESC;

-- 5. List of stock items that have at least 10 characters in description.
SELECT S.StockItemName
FROM Purchasing.PurchaseOrderLines P
	INNER JOIN Warehouse.StockItems S
	ON P.StockItemID = S.StockItemID
WHERE LEN(P.Description) > 9
UNION
SELECT S.StockItemName
FROM Sales.InvoiceLines I
	INNER JOIN Warehouse.StockItems S
	ON I.StockItemID = S.StockItemID
WHERE LEN(I.Description) > 9
UNION
SELECT S.StockItemName
FROM Sales.OrderLines O
	INNER JOIN Warehouse.StockItems S
	ON O.StockItemID = S.StockItemID
WHERE LEN(O.Description) > 9;

-- 6. List of stock items that are not sold to the state of Alabama and Georgia in 2014.
WITH cte_TransactionState AS (
	SELECT T.InvoiceID, T.TransactionDate, S.StateProvinceName
	FROM Sales.CustomerTransactions T
		LEFT JOIN Sales.Customers Cu
		ON T.CustomerID = Cu.CustomerID
		LEFT JOIN Application.Cities Ci
		ON Cu.DeliveryCityID = Ci.CityID
		LEFT JOIN Application.StateProvinces S
		ON Ci.StateProvinceID = S.StateProvinceID
)
SELECT DISTINCT S.StockItemName
FROM cte_TransactionState CTE
	INNER JOIN Sales.InvoiceLines I
	ON CTE.InvoiceID = I.InvoiceID
	INNER JOIN Warehouse.StockItems S
	ON I.StockItemID = S.StockItemID
WHERE YEAR(CTE.TransactionDate) = '2014'
	AND CTE.StateProvinceName NOT IN ('Alabama', 'Georgia');

-- 7. List of States and Avg dates for processing (confirmed delivery date – order date).
SELECT S.StateProvinceName, 
	AVG(DATEDIFF(DAY, O.OrderDate, I.ConfirmedDeliveryTime)) AS AvgProcessing
FROM Sales.Orders O
	INNER JOIN Sales.Invoices I
	ON O.OrderID = I.OrderID
	INNER JOIN Sales.Customers Cu
	ON I.CustomerID = Cu.CustomerID
	RIGHT JOIN Application.Cities Ci
	ON Cu.DeliveryCityID = Ci.CityID
	INNER JOIN Application.StateProvinces S
	ON Ci.StateProvinceID = S.StateProvinceID
GROUP BY S.StateProvinceName
ORDER BY AvgProcessing DESC;

-- 8. List of States and Avg dates for processing (confirmed delivery date – order date) by month.
SELECT MONTH(O.OrderDate) AS MonthOrdered, S.StateProvinceName, 
	AVG(DATEDIFF(DAY, O.OrderDate, I.ConfirmedDeliveryTime)) AS AvgProcessing
FROM Sales.Orders O
	INNER JOIN Sales.Invoices I
	ON O.OrderID = I.OrderID
	INNER JOIN Sales.Customers Cu
	ON I.CustomerID = Cu.CustomerID
	RIGHT JOIN Application.Cities Ci
	ON Cu.DeliveryCityID = Ci.CityID
	INNER JOIN Application.StateProvinces S
	ON Ci.StateProvinceID = S.StateProvinceID
GROUP BY MONTH(O.OrderDate), S.StateProvinceName
HAVING MONTH(O.OrderDate) IS NOT NULL
ORDER BY MonthOrdered ASC, AvgProcessing DESC;

-- 9. List of StockItems that the company purchased more than sold in the year of 2015.
WITH cte_PurchasedSold AS (
	SELECT OrderedOuters, Quantity, StockItemID
	FROM (
		SELECT P.OrderedOuters, Si.StockItemID
		FROM Purchasing.SupplierTransactions St
			INNER JOIN Purchasing.PurchaseOrderLines P
			ON St.PurchaseOrderID = P.PurchaseOrderID
			INNER JOIN Warehouse.StockItems Si
			ON P.StockItemID = Si.StockItemID
		WHERE YEAR(St.TransactionDate) = '2015'
	) sub_Purchased
	INNER JOIN (
		SELECT I.Quantity, S.StockItemID AS SID
		FROM Sales.CustomerTransactions C
			INNER JOIN Sales.InvoiceLines I
			ON C.InvoiceID = I.InvoiceID
			INNER JOIN Warehouse.StockItems S
			ON I.StockItemID = S.StockItemID
		WHERE YEAR(C.TransactionDate) = '2015'
	) sub_Sold
	ON sub_Purchased.StockItemID = sub_Sold.SID
)
SELECT S.StockItemName
FROM cte_PurchasedSold CTE
	INNER JOIN Warehouse.StockItems S
	ON CTE.StockItemID = S.StockItemID
GROUP BY S.StockItemName
HAVING SUM(CTE.OrderedOuters) > SUM(CTE.Quantity);

-- 10. List of Customers and their phone number, together with the primary contact person’s name, to 
-- whom we did not sell more than 10 mugs (search by name) in the year 2016.
WITH cte_RelevantCustomers AS (
	SELECT C.CustomerID
	FROM Sales.CustomerTransactions C
		INNER JOIN Sales.InvoiceLines I
		ON C.InvoiceID = I.InvoiceID
		INNER JOIN Warehouse.StockItems S
		ON I.StockItemID = S.StockItemID
	WHERE YEAR(C.TransactionDate) = '2016' AND S.StockItemName LIKE N'%mug%'
	GROUP BY C.CustomerID
	HAVING SUM(I.Quantity) < 11
)
SELECT C.CustomerName, C.PhoneNumber, P.FullName AS PrimaryContact
FROM cte_RelevantCustomers CTE
	INNER JOIN Sales.Customers C
	ON CTE.CustomerID = C.CustomerID
	INNER JOIN Application.People P
	ON C.PrimaryContactPersonID = P.PersonID;

-- 11. List all the cities that were updated after 2015-01-01.
SELECT C.CityName, S.StateProvinceName
FROM Application.Cities C
	INNER JOIN Application.StateProvinces S
	ON C.StateProvinceID = S.StateProvinceID
WHERE C.ValidFrom > '2015-01-01';

-- 12. List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, 
-- customer name, customer contact person name, customer phone, quantity) for the date of 2014-07-01. 
-- Info should be relevant to that date.
WITH cte_CustomerDetails AS (
	SELECT Cu.DeliveryAddressLine2 + ', ' + Ci.CityName + ', ' + S.StateProvinceCode + ' ' + Cu.DeliveryPostalCode AS DeliveryAddress,
		Ci.CityName AS DeliveryCity, S.StateProvinceName AS DeliveryState, Co.CountryName AS DeliveryCountry,
		Cu.CustomerName, P.FullName AS ContactName, Cu.PhoneNumber, Ct.InvoiceID
	FROM Sales.CustomerTransactions Ct
		LEFT JOIN Sales.Customers Cu
		ON Ct.CustomerID = Cu.CustomerID
		INNER JOIN Application.Cities Ci
		ON Cu.DeliveryCityID = Ci.CityID
		INNER JOIN Application.StateProvinces S
		ON Ci.StateProvinceID = S.StateProvinceID
		INNER JOIN Application.Countries Co
		ON S.CountryID = Co.CountryID
		INNER JOIN Application.People P
		ON Cu.PrimaryContactPersonID = P.PersonID
	WHERE Ct.TransactionDate = '2014-07-01'
)
SELECT S.StockItemName, CTE.DeliveryAddress, CTE.DeliveryCity, CTE.DeliveryState, CTE.DeliveryCountry,
	CTE.CustomerName, CTE.ContactName, CTE.PhoneNumber, I.Quantity
FROM cte_CustomerDetails CTE
	INNER JOIN Sales.InvoiceLines I
	ON CTE.InvoiceID = I.InvoiceID
	INNER JOIN Warehouse.StockItems S
	ON I.StockItemID = S.StockItemID;

-- 13. List of stock item groups and total quantity purchased, total quantity sold, and the remaining 
-- stock quantity (quantity purchased – quantity sold)
WITH cte_PurchasedSold AS (
	SELECT OrderedOuters, Quantity, StockItemID
	FROM (
		SELECT P.OrderedOuters, Si.StockItemID
		FROM Purchasing.SupplierTransactions St
			INNER JOIN Purchasing.PurchaseOrderLines P
			ON St.PurchaseOrderID = P.PurchaseOrderID
			INNER JOIN Warehouse.StockItems Si
			ON P.StockItemID = Si.StockItemID
	) sub_Purchased
	INNER JOIN (
		SELECT I.Quantity, S.StockItemID AS SID
		FROM Sales.CustomerTransactions C
			INNER JOIN Sales.InvoiceLines I
			ON C.InvoiceID = I.InvoiceID
			INNER JOIN Warehouse.StockItems S
			ON I.StockItemID = S.StockItemID
	) sub_Sold
	ON sub_Purchased.StockItemID = sub_Sold.SID
)
SELECT Sg.StockGroupName, 
	SUM(CAST(CTE.OrderedOuters AS BIGINT)) AS TotalPurchased, 
	SUM(CAST(CTE.Quantity AS BIGINT)) AS TotalSold,
	SUM(CAST(CTE.OrderedOuters - CTE.Quantity AS BIGINT)) AS Remaining
FROM cte_PurchasedSold CTE
	INNER JOIN Warehouse.StockItemStockGroups Si
	ON CTE.StockItemID = Si.StockItemID
	INNER JOIN Warehouse.StockGroups Sg
	ON Si.StockGroupID = Sg.StockGroupID
GROUP BY Sg.StockGroupName;

-- 14. List of Cities in the US and the stock item that the city got the most deliveries in 2016. 
-- If the city did not purchase any stock items in 2016, print “No Sales”.
WITH cte_StockItemCities AS (
	SELECT Ci.CityName + ', ' + Sp.StateProvinceCode AS City, 
		Si.StockItemID, COUNT(Si.StockItemID) AS StockItemCount
	FROM Warehouse.StockItemTransactions Si
		INNER JOIN Sales.Customers Cu
		ON Si.CustomerID = Cu.CustomerID
		RIGHT JOIN Application.Cities Ci
		ON Cu.DeliveryCityID = Ci.CityID
		INNER JOIN Application.StateProvinces Sp
		ON Ci.StateProvinceID = Sp.StateProvinceID
	WHERE YEAR(Si.TransactionOccurredWhen) = '2016' AND Sp.CountryID = 230
	GROUP BY Ci.CityName + ', ' + Sp.StateProvinceCode, Si.StockItemID
	UNION
	SELECT Ci.CityName + ', ' + Sp.StateProvinceCode AS City, 
		Si.StockItemID, COUNT(Si.StockItemID) AS StockItemCount
	FROM Warehouse.StockItemTransactions Si
		INNER JOIN Purchasing.Suppliers Su
		ON Si.SupplierID = Su.SupplierID
		RIGHT JOIN Application.Cities Ci
		ON Su.DeliveryCityID = Ci.CityID
		INNER JOIN Application.StateProvinces Sp
		ON Ci.StateProvinceID = Sp.StateProvinceID
	WHERE YEAR(Si.TransactionOccurredWhen) = '2016' AND Sp.CountryID = 230
	GROUP BY Ci.CityName + ', ' + Sp.StateProvinceCode, Si.StockItemID
)
SELECT sub_Ranked.City, COALESCE(S.StockItemName, 'No Sales') AS MostDelivered
FROM (
	SELECT *,
		DENSE_RANK() OVER (PARTITION BY City ORDER BY StockItemCount DESC) Ranking
	FROM cte_StockItemCities
) sub_Ranked
	LEFT JOIN Warehouse.StockItems S
	ON sub_Ranked.StockItemID = S.StockItemID
WHERE sub_Ranked.Ranking = 1

-- 15. List any orders that had more than one delivery attempt (located in invoice table).
SELECT O.*
FROM Sales.Invoices I
	INNER JOIN Sales.Orders O
	ON I.OrderID = O.OrderID
WHERE JSON_QUERY(I.ReturnedDeliveryData, '$."Events"') LIKE N'%DeliveryAttempt%DeliveryAttempt%';

-- 16. List all stock items that are manufactured in China. (Country of Manufacture)
SELECT DISTINCT StockItemName
FROM Warehouse.StockItems
WHERE JSON_VALUE(CustomFields, '$."CountryOfManufacture"') = 'China';

-- 17. Total quantity of stock items sold in 2015, group by country of manufacturing.
SELECT JSON_VALUE(S.CustomFields, '$."CountryOfManufacture"') AS CountryOfManufacture,
	SUM(I.Quantity) AS TotalQuantity
FROM Sales.CustomerTransactions C
	INNER JOIN Sales.InvoiceLines I
	ON C.InvoiceID = I.InvoiceID
	INNER JOIN Warehouse.StockItems S
	ON I.StockItemID = S.StockItemID
WHERE YEAR(C.TransactionDate) = '2015'
GROUP BY JSON_VALUE(S.CustomFields, '$."CountryOfManufacture"')
ORDER BY TotalQuantity DESC;