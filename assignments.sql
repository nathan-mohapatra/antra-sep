USE WideWorldImporters;

-- 1. List of Persons’ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any).
SELECT P.FullName, P.PhoneNumber AS PersonalPhone, P.FaxNumber AS PersonalFax, 
	COALESCE(C.PhoneNumber, S.PhoneNumber) AS CompanyPhone, COALESCE(C.FaxNumber, S.FaxNumber) AS CompanyFax
FROM Application.People P
	LEFT JOIN Sales.Customers C 
	ON (P.PersonID = C.PrimaryContactPersonID) OR (P.PersonID = C.AlternateContactPersonID)
	LEFT JOIN Purchasing.Suppliers S 
	ON (P.PersonID = S.PrimaryContactPersonID) OR (P.PersonID = S.AlternateContactPersonID)
WHERE P.FullName != 'Data Conversion Only';

-- 2. If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies.
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