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
WHERE JSON_QUERY(I.ReturnedDeliveryData, '$.Events') LIKE N'%DeliveryAttempt%DeliveryAttempt%';

-- 16. List all stock items that are manufactured in China. (Country of Manufacture)
SELECT DISTINCT StockItemName
FROM Warehouse.StockItems
WHERE JSON_VALUE(CustomFields, '$.CountryOfManufacture') = 'China';

-- 17. Total quantity of stock items sold in 2015, group by country of manufacturing.
SELECT JSON_VALUE(S.CustomFields, '$.CountryOfManufacture') AS CountryOfManufacture,
	SUM(I.Quantity) AS TotalQuantity
FROM Sales.CustomerTransactions C
	INNER JOIN Sales.InvoiceLines I
	ON C.InvoiceID = I.InvoiceID
	INNER JOIN Warehouse.StockItems S
	ON I.StockItemID = S.StockItemID
WHERE YEAR(C.TransactionDate) = '2015'
GROUP BY JSON_VALUE(S.CustomFields, '$.CountryOfManufacture')
ORDER BY TotalQuantity DESC;

-- 18. Create a view that shows the total quantity of stock items of each stock group sold (in orders) 
-- by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]
IF OBJECT_ID('Sales.vStockItemGroupSales1', 'view') IS NOT NULL
	DROP VIEW Sales.vStockItemGroupSales1;
GO
CREATE VIEW Sales.vStockItemGroupSales1
	WITH SCHEMABINDING
	AS
		SELECT StockGroupName,
			[2013], [2014], [2015], [2016], [2017]
		FROM (
			SELECT Sg.StockGroupName, YEAR(C.TransactionDate) AS TransactionYear,
				SUM(I.Quantity) AS TotalQuantity
			FROM Sales.CustomerTransactions C
				INNER JOIN Sales.InvoiceLines I
				ON C.InvoiceID = I.InvoiceID
				INNER JOIN Warehouse.StockItems S
				ON I.StockItemID = S.StockItemID
				INNER JOIN Warehouse.StockItemStockGroups Si
				ON S.StockItemID = Si.StockItemID
				INNER JOIN Warehouse.StockGroups Sg
				ON Si.StockGroupID = Sg.StockGroupID
			WHERE YEAR(C.TransactionDate) BETWEEN '2013' AND '2017'
			GROUP BY Sg.StockGroupName, YEAR(C.TransactionDate)
		) SourceTable
		PIVOT (
			SUM(TotalQuantity)
			FOR TransactionYear IN ([2013], [2014], [2015], [2016], [2017])
		) PivotTable;
GO
-- 19. Create a view that shows the total quantity of stock items of each stock group sold (in orders) 
-- by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, …, Stock Group Name10]
IF OBJECT_ID('Sales.vStockItemGroupSales2', 'view') IS NOT NULL
	DROP VIEW Sales.vStockItemGroupSales2;
GO
CREATE VIEW Sales.vStockItemGroupSales2
	WITH SCHEMABINDING
	AS
		SELECT TransactionYear,
			[Clothing], [Computing Novelties], [Furry Footwear],  [Mugs], [Novelty Items], 
			[Packaging Materials], [Toys], [T-Shirts], [USB Novelties]
		FROM (
			SELECT YEAR(C.TransactionDate) AS TransactionYear, Sg.StockGroupName, 
			SUM(I.Quantity) AS TotalQuantity
			FROM Sales.CustomerTransactions C
				INNER JOIN Sales.InvoiceLines I
				ON C.InvoiceID = I.InvoiceID
				INNER JOIN Warehouse.StockItems S
				ON I.StockItemID = S.StockItemID
				INNER JOIN Warehouse.StockItemStockGroups Si
				ON S.StockItemID = Si.StockItemID
				INNER JOIN Warehouse.StockGroups Sg
				ON Si.StockGroupID = Sg.StockGroupID
			WHERE YEAR(C.TransactionDate) BETWEEN '2013' AND '2017'
			GROUP BY YEAR(C.TransactionDate), Sg.StockGroupName
		) SourceTable
		PIVOT (
			SUM(TotalQuantity)
			FOR StockGroupName IN ([Clothing], [Computing Novelties], [Furry Footwear],  [Mugs], [Novelty Items], 
				[Packaging Materials], [Toys], [T-Shirts], [USB Novelties])
		) PivotTable;
GO
-- 20. Create a function, input: order id; return: total of that order. List invoices and use that 
-- function to attach the order total to the other fields of invoices.
CREATE FUNCTION dbo.udfGetOrderTotal(@OrderID INT)
RETURNS MONEY
AS
BEGIN
	DECLARE @ret MONEY;
	SELECT @ret = O.UnitPrice + (O.UnitPrice * O.TaxRate)
	FROM Sales.OrderLines O
	WHERE O.OrderID = @OrderID
	RETURN @ret;
END;
GO
SELECT *
FROM Sales.Invoices I
CROSS APPLY (
	SELECT dbo.udfGetOrderTotal(I.OrderID) AS OrderTotal
) func;
GO
-- 21. Create a new table called ods.Orders. Create a stored procedure, with proper error handling 
-- and transactions, that input is a date; when executed, it would find orders of that day, calculate 
-- order total, and save the information (order id, order date, order total, customer id) into the 
-- new table. If a given date is already existing in the new table, throw an error and roll back. 
-- Execute the stored procedure 5 times using different dates.
CREATE SCHEMA ods;
GO
DROP TABLE IF EXISTS ods.Orders;
CREATE TABLE ods.Orders(
	OrderID INT NOT NULL PRIMARY KEY,
	OrderDate DATE NOT NULL,
	OrderTotal MONEY NOT NULL,
	CustomerID INT NOT NULL,
	CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID)
		REFERENCES Sales.Customers (CustomerID)
);
IF OBJECT_ID('ods.uspGetOrdersByDate', 'P') IS NOT NULL
	DROP PROCEDURE ods.uspGetOrdersByDate;
GO
CREATE PROCEDURE ods.uspGetOrdersByDate
	@OrderDate DATE
AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY
		BEGIN TRANSACTION InsertRelevantOrders
			IF EXISTS (SELECT 1 FROM ods.Orders O WHERE O.OrderDate = @OrderDate)
				BEGIN;
					THROW 50000, 'Stored procedure has already been executed with this date!', 1
					ROLLBACK TRANSACTION InsertRelevantOrders;
				END
			INSERT INTO ods.Orders
			SELECT O.OrderID, O.OrderDate, 
				Ol.UnitPrice + (Ol.UnitPrice * Ol.TaxRate) AS OrderTotal, O.CustomerID
			FROM Sales.Orders O
				INNER JOIN Sales.OrderLines Ol
				ON O.OrderID = Ol.OrderID
			WHERE O.OrderDate = @OrderDate;
		COMMIT TRANSACTION InsertRelevantOrders
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			BEGIN
				ROLLBACK TRANSACTION InsertRelevantOrders;
			END
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	END CATCH
END;
EXECUTE ods.uspGetOrdersByDate @OrderDate = '2013-01-01';
EXECUTE ods.uspGetOrdersByDate @OrderDate = '2013-01-02';
EXECUTE ods.uspGetOrdersByDate @OrderDate = '2013-01-03';
EXECUTE ods.uspGetOrdersByDate @OrderDate = '2013-01-04';
EXECUTE ods.uspGetOrdersByDate @OrderDate = '2013-01-05';

-- 22. Create a new table called ods.StockItem. It has following columns: [StockItemID], 
-- [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,
-- [LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],
-- [RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments]  ,[InternalComments], 
-- [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table.
DROP TABLE IF EXISTS ods.StockItems;
CREATE TABLE ods.StockItems(
	StockItemID INT NOT NULL PRIMARY KEY,
	StockItemName NVARCHAR(100) NOT NULL,
	SupplierID INT NOT NULL,
	ColorID INT,
	UnitPackageID INT NOT NULL,
	OuterPackageID INT NOT NULL,
	Brand NVARCHAR(50),
	Size NVARCHAR(20),
	LeadTimeDays INT NOT NULL,
	QuantityPerOuter INT NOT NULL,
	IsChillerStock BIT NOT NULL,
	Barcode NVARCHAR(50),
	TaxRate DECIMAL(18,3) NOT NULL,
	UnitPrice DECIMAL(18,2) NOT NULL,
	RecommendedRetailPrice DECIMAL(18,2),
	TypicalWeightBeforeUnit DECIMAL(18,3) NOT NULL,
	MarketingComments NVARCHAR(MAX),
	InternalComments NVARCHAR(MAX),
	CountryOfManufacture NVARCHAR(50) NOT NULL,
	Range INT NOT NULL,
	Shelflife INT NOT NULL,
	CONSTRAINT FK_StockItems_Suppliers FOREIGN KEY (SupplierID)
		REFERENCES Purchasing.Suppliers (SupplierID),
	CONSTRAINT FK_StockItems_Colors FOREIGN KEY (ColorID)
		REFERENCES Warehouse.Colors (ColorID),
	CONSTRAINT FK_StockItems_UPackages FOREIGN KEY (UnitPackageID)
		REFERENCES Warehouse.PackageTypes (PackageTypeID),
	CONSTRAINT FK_StockItems_OPackages FOREIGN KEY (OuterPackageID)
		REFERENCES Warehouse.PackageTypes (PackageTypeID)
);
INSERT INTO ods.StockItems
SELECT StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, 
	IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, MarketingComments, InternalComments, 
	JSON_VALUE(CustomFields, '$."CountryOfManufacture"'), DATEDIFF(DAY, ValidFrom, ValidTo), DATEDIFF(MONTH, ValidFrom, ValidTo)
FROM Warehouse.StockItems;
GO
-- 23. Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order 
-- data prior to the input date and load the order data that was placed in the next 7 days following 
-- the input date.
ALTER PROCEDURE ods.uspGetOrdersByDate
	@OrderDate DATE
AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY
		BEGIN TRANSACTION DeleteOldInsertRecent
			DELETE O FROM ods.Orders O
			WHERE O.OrderDate < @OrderDate;
			INSERT INTO ods.Orders
			SELECT O.OrderID, O.OrderDate, 
				Ol.UnitPrice + (Ol.UnitPrice * Ol.TaxRate) AS OrderTotal, O.CustomerID
			FROM Sales.Orders O
				INNER JOIN Sales.OrderLines Ol
				ON O.OrderID = Ol.OrderID
			WHERE DATEDIFF(DAY, @OrderDate, O.OrderDate) BETWEEN 0 AND 7;
		COMMIT TRANSACTION DeleteOldInsertRecent
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			BEGIN
				ROLLBACK TRANSACTION DeleteOldInsertRecent;
			END
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	END CATCH
END;

-- 24. Consider the given JSON file. Looks like that it is our missed purchase orders. Migrate these 
-- data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.
DECLARE @json NVARCHAR(MAX)
SET @json = '{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":"6",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}'
INSERT INTO Warehouse.StockItems
SELECT (SELECT MAX(StockItemID) + 1 FROM Warehouse.StockItems), StockItemName, SupplierID, NULL, UnitPackageID, OuterPackageID, Brand, NULL, 
	LeadTimeDays, QuantityPerOuter, 0, NULL, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, NULL, NULL, NULL,
	'{"CountryOfManufacture":"' + CountryOfManufacture + '"}', '[]', 'None', 1, '2016-05-31 23:00:00.0000000', '9999-12-31 23:59:59.9999999'
FROM OPENJSON(@json) WITH (
	StockItemName NVARCHAR(100) '$.PurchaseOrders[0].StockItemName',
	SupplierID INT '$.PurchaseOrders[0].Supplier',
	UnitPackageID INT '$.PurchaseOrders[0].UnitPackageId',
	OuterPackageID INT '$.PurchaseOrders[0].OuterPackageId',
	Brand NVARCHAR(50) '$.PurchaseOrders[0].Brand',
	LeadTimeDays INT '$.PurchaseOrders[0].LeadTimeDays',
	QuantityPerOuter INT '$.PurchaseOrders[0].QuantityPerOuter',
	TaxRate DECIMAL(18,3) '$.PurchaseOrders[0].TaxRate',
	UnitPrice DECIMAL(18,2) '$.PurchaseOrders[0].UnitPrice',
	RecommendedRetailPrice DECIMAL(18,2) '$.PurchaseOrders[0].RecommendedRetailPrice',
	TypicalWeightPerUnit DECIMAL(18,3) '$.PurchaseOrders[0].TypicalWeightPerUnit',
	CountryOfManufacture NVARCHAR(50) '$.PurchaseOrders[0].CountryOfManufacture'
);
INSERT INTO Purchasing.PurchaseOrders
SELECT (SELECT MAX(PurchaseOrderID) FROM Purchasing.PurchaseOrders), ABS(CHECKSUM(NEWID()) % 13) + 1, OrderDate, 1, 2, ExpectedDeliveryDate, 
	SupplierReference, 1, NULL, NULL, ABS(CHECKSUM(NEWID()) % 20) + 1, DATEADD(DAY, 1, OrderDate)
FROM OPENJSON(@json) WITH (
	OrderDate DATE '$.PurchaseOrders[0].OrderDate',
	ExpectedDeliveryDate DATE '$.PurchaseOrders[0].ExpectedDeliveryDate',
	SupplierReference NVARCHAR(20) '$.PurchaseOrders[0].SupplierReference'
);
INSERT INTO Warehouse.StockItems
SELECT (SELECT MAX(StockItemID) + 1 FROM Warehouse.StockItems), StockItemName, SupplierID, NULL, UnitPackageID, OuterPackageID, Brand, NULL, 
	LeadTimeDays, QuantityPerOuter, 0, NULL, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, NULL, NULL, NULL,
	'{"CountryOfManufacture":"' + CountryOfManufacture + '"}', '[]', 'None', 1, '2016-05-31 23:00:00.0000000', '9999-12-31 23:59:59.9999999'
FROM OPENJSON(@json) WITH (
	StockItemName NVARCHAR(100) '$.PurchaseOrders[1].StockItemName',
	SupplierID INT '$.PurchaseOrders[1].Supplier',
	UnitPackageID INT '$.PurchaseOrders[1].UnitPackageId',
	OuterPackageID INT '$.PurchaseOrders[1].OuterPackageId',
	Brand NVARCHAR(50) '$.PurchaseOrders[1].Brand',
	LeadTimeDays INT '$.PurchaseOrders[1].LeadTimeDays',
	QuantityPerOuter INT '$.PurchaseOrders[1].QuantityPerOuter',
	TaxRate DECIMAL(18,3) '$.PurchaseOrders[1].TaxRate',
	UnitPrice DECIMAL(18,2) '$.PurchaseOrders[1].UnitPrice',
	RecommendedRetailPrice DECIMAL(18,2) '$.PurchaseOrders[1].RecommendedRetailPrice',
	TypicalWeightPerUnit DECIMAL(18,3) '$.PurchaseOrders[1].TypicalWeightPerUnit',
	CountryOfManufacture NVARCHAR(50) '$.PurchaseOrders[1].CountryOfManufacture'
);
INSERT INTO Purchasing.PurchaseOrders
SELECT (SELECT MAX(PurchaseOrderID) FROM Purchasing.PurchaseOrders), ABS(CHECKSUM(NEWID()) % 13) + 1, OrderDate, 1, 2, ExpectedDeliveryDate, 
	SupplierReference, 1, NULL, NULL, ABS(CHECKSUM(NEWID()) % 20) + 1, DATEADD(DAY, 1, OrderDate)
FROM OPENJSON(@json) WITH (
	OrderDate DATE '$.PurchaseOrders[1].OrderDate',
	ExpectedDeliveryDate DATE '$.PurchaseOrders[1].ExpectedDeliveryDate',
	SupplierReference NVARCHAR(20) '$.PurchaseOrders[1].SupplierReference'
);

-- 25. Revisit your answer in (19). Convert the result in JSON string and save it to the server using 
-- TSQL FOR JSON PATH.
SELECT TransactionYear,
	[Clothing], [Computing Novelties], [Furry Footwear],  [Mugs], [Novelty Items], 
	[Packaging Materials], [Toys], [T-Shirts], [USB Novelties]
FROM (
	SELECT YEAR(C.TransactionDate) AS TransactionYear, Sg.StockGroupName, 
	SUM(I.Quantity) AS TotalQuantity
	FROM Sales.CustomerTransactions C
		INNER JOIN Sales.InvoiceLines I
		ON C.InvoiceID = I.InvoiceID
		INNER JOIN Warehouse.StockItems S
		ON I.StockItemID = S.StockItemID
		INNER JOIN Warehouse.StockItemStockGroups Si
		ON S.StockItemID = Si.StockItemID
		INNER JOIN Warehouse.StockGroups Sg
		ON Si.StockGroupID = Sg.StockGroupID
	WHERE YEAR(C.TransactionDate) BETWEEN '2013' AND '2017'
	GROUP BY YEAR(C.TransactionDate), Sg.StockGroupName
) SourceTable
PIVOT (
	SUM(TotalQuantity)
	FOR StockGroupName IN ([Clothing], [Computing Novelties], [Furry Footwear],  [Mugs], [Novelty Items], 
		[Packaging Materials], [Toys], [T-Shirts], [USB Novelties])
) PivotTable
FOR JSON PATH;

-- 26. Revisit your answer in (19). Convert the result into an XML string and save it to the server 
-- using TSQL FOR XML PATH.
SELECT TransactionYear,
	[Clothing], [Computing Novelties] AS ComputingNovelties, [Furry Footwear] AS FurryFootwear,  [Mugs], [Novelty Items] AS NoveltyItems, 
	[Packaging Materials] AS PackagingMaterials, [Toys], [T-Shirts], [USB Novelties] AS USBNovelties
FROM (
	SELECT YEAR(C.TransactionDate) AS TransactionYear, Sg.StockGroupName, 
	SUM(I.Quantity) AS TotalQuantity
	FROM Sales.CustomerTransactions C
		INNER JOIN Sales.InvoiceLines I
		ON C.InvoiceID = I.InvoiceID
		INNER JOIN Warehouse.StockItems S
		ON I.StockItemID = S.StockItemID
		INNER JOIN Warehouse.StockItemStockGroups Si
		ON S.StockItemID = Si.StockItemID
		INNER JOIN Warehouse.StockGroups Sg
		ON Si.StockGroupID = Sg.StockGroupID
	WHERE YEAR(C.TransactionDate) BETWEEN '2013' AND '2017'
	GROUP BY YEAR(C.TransactionDate), Sg.StockGroupName
) SourceTable
PIVOT (
	SUM(TotalQuantity)
	FOR StockGroupName IN ([Clothing], [Computing Novelties], [Furry Footwear],  [Mugs], [Novelty Items], 
		[Packaging Materials], [Toys], [T-Shirts], [USB Novelties])
) PivotTable
FOR XML PATH;
GO
-- 27. Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value). Create a 
-- stored procedure, input is a date. The logic would load invoice information (all columns) as well 
-- as invoice line information (all columns) and forge them into a JSON string and then insert into 
-- the new table just created. Then write a query to run the stored procedure for each DATE that 
-- customer id 1 got something delivered to him.
-- 27. Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value). Create a 
-- stored procedure, input is a date. The logic would load invoice information (all columns) as well 
-- as invoice line information (all columns) and forge them into a JSON string and then insert into 
-- the new table just created. Then write a query to run the stored procedure for each DATE that 
-- customer id 1 got something delivered to him.
DROP TABLE IF EXISTS ods.ConfirmedDeliveryJson;
CREATE TABLE ods.ConfirmedDeliveryJson(
	ConfirmedDeliveryID INT NOT NULL IDENTITY (1,1) PRIMARY KEY,
	ConfirmedDeliveryDate DATE NOT NULL,
	ConfirmedDeliveryInfo NVARCHAR(MAX) NOT NULL
);
IF OBJECT_ID('ods.uspGetConfirmedDeliveries', 'P') IS NOT NULL
	DROP PROCEDURE ods.uspGetConfirmedDeliveries;
GO
CREATE PROCEDURE ods.uspGetConfirmedDeliveries
	@InvoiceDate DATE
AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY
		BEGIN TRANSACTION LoadInvoiceData
			INSERT INTO ods.ConfirmedDeliveryJson
			SELECT InvoiceDate, 
				(SELECT * FROM Sales.Invoices I INNER JOIN Sales.InvoiceLines IL 
				ON I.InvoiceID = IL.InvoiceID FOR JSON AUTO)
			FROM Sales.Invoices
			WHERE InvoiceDate = @InvoiceDate;
		COMMIT TRANSACTION LoadInvoiceData
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			BEGIN
				ROLLBACK TRANSACTION LoadInvoiceData;
			END
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	END CATCH
END;
DECLARE @RelevantInvoiceDate DATE
DECLARE cur CURSOR LOCAL FOR
	SELECT InvoiceDate FROM Sales.Invoices WHERE CustomerID = 1
OPEN cur
FETCH NEXT FROM cur INTO @RelevantInvoiceDate
WHILE @@FETCH_STATUS = 0 BEGIN
	EXECUTE ods.uspGetConfirmedDeliveries @InvoiceDate = @RelevantInvoiceDate
	FETCH NEXT FROM cur INTO @RelevantInvoiceDate
END
CLOSE cur
DEALLOCATE cur;

-- 28. Write a short essay talking about your understanding of transactions, locks and isolation levels.
/*
    A transaction is a sequence of operations performed (using one or more SQL statements) on a 
database as a single logical unit of work. ACID is an ancronym that describes the fundamental
properties of a transaction; it stands for Atomicity, Consistency, Isolation, and Durability.
Atomicity ensures that each statement in a transaction (to read, write, update, or delete data) is
treated as a single unit. Either the entire statement is executed, or none of it is executed. This
property prevents data loss and corruption from occurring if, for example, your streaming data source
fails mid-stream. Consistency ensures that transactions only make changes to tables in predefined, 
predictable ways. Transactional consistency ensures that corruption or errors in your data do not 
create unintended consequences for the integrity of your table. Isolation ensures that the concurrent 
transactions do not interfere with or affect one another when multiple users are reading and writing 
from the same table all at once. Each request can occur as though they were occurring one by one, even 
though they are actually occurring simultaneously. Durability ensures that changes to your data made 
by successfully executed transactions will be saved, even in the event of system failure.

    A transaction has two outcomes: committed or rolled back. The `@@TRANSCOUNT` function returns the
number of `BEGIN TRANSACTION` statements in the current session; it can be used to count the number
of open local transactions. Autocommit transactions are the default transaction for Microsoft SQL
Server. Each T-SQL statement is evaluated as a transaction and committed or rolled back according
to its results. Successful and failed statements are committed and rolled back, respectively. Implicit 
transactions enable Microsoft SQL Server to start a transaction for every Data Manipulation Language
(DML) statement, but explicit commit or rollback commands are needed at the end of statements.
Explicit transactions define a transaction exactly, with the starting and ending points of the
transaction specified. There are also batch-scoped transactions.

    Locks are held on Microsoft SQL Server resources, such as rows read or modified during a
transaction, to prevent concurrent use of resources by different transactions (and the resulting
concurrency issues). Isolation levels define how one transaction is isolated from other transactions,
with isolation being the separation of resource or data modification made by different transactions.
Isolation levels are described by which concurrency issues they allow, and control whether locks are
taken, what types of locks are requested, or how long locks are held. The tradeoff between the risk
of rolling back transactions and waiting times is encapsulated by optimistic versus pessimistic
concurrency control, two philosophies that must be considered. With optimistic concurrency control, 
the approach is "Let's not use too many locks and hope for the best." The readers cannot block 
writers, the writers cannot block readers, but the writer can block another writer. When a user 
updates data, the system checks to see if another user changed the data after it was read. If another
user updated the data, an error is raised. Typically, the user receiving the error rolls back the
transaction and starts over. Timestamps are mainly used to check the version of data and, when
committing, the newest version will be used and others will need to roll back. With pessimistic
concurrency control, the approach is "Let's be careful and try to avoid concurrency issues." After a 
user performs an action that causes a lock to be applied, other users cannot perform actions that
would conflict with the lock until the owner releases it. A system of locks prevent users from
modifying data in a way that affects other users.

    There are different isolation levels: Snapshot, Read Uncommitted, Read Commited (system default), 
Repeatable Read, and Serializable. Snapshot follows the model of optimistic concurrency control by 
avoiding most locking and blocks by using row versioning. When data is modified, the committed
versions of affected rows are copied to `tempdb` and given version numbers. This operation is called 
copy on write and is used for all inserts, updates, and deletes. When another session reads the same
data, the committed version of the data as of the time of the reading transaction began is returned.
Read Uncommitted is the first level of isolation, and it comes under the model of pessimistic
concurrency control. One transaction is allowed to read the data that is about to be changed by the
commit of another process, allowing the Dirty Read problem (i.e. another process reads the changed,
but uncommitted data). Read Committed is the next level of isolation under this model. It only allows
the reading of data that is committed, eliminating the Dirty Read problem. Repeatable Read is the next
level of isolation under this model, and eliminates the Non-repeatable Read problem (i.e. one process
is reading the data, and another process is writing to the data). The transaction has to wait until
the read query or update of another transaction is complete. However, there is no waiting for an
insert transaction, allowing the Phantom Read problem (i.e. two identical queries executed by two 
different users show different output). Serialization is the highest level of isolation under this 
model. Any transaction can be asked to wait until the current transaction completes, preventing the 
Read Phantom problem.

    There are many different types of locks, including but not limited to: Exclusive (X), Shared (S),
Update (U), Intent (I), Schema (Sch), and Bulk Update (BU). Exclusive (X) will ensure that a page or 
row will be reserved exclusively for the transaction that imposed the exclusive lock, as long as the 
transaction holds the lock. Shared (S) will reserve a page or row to be available only for reading,
and can be posed by several transactions at the same time. It will allow write operations, but no
Data Definition Language (DDL) changes will be allowed. Update (U) is similar to an exclusive lock, 
but is designed to be more flexible in a way. It can be imposed on a record that already has a shared
lock; in this case, the update lock will impose another shared lock on the target row. Once the 
transaction that holds the update lock is ready to change the data, the update lock will be 
transformed into an exclusive lock. Intent (I) is used by a transaction to inform another transaction
about its intention to acquire a lock. The purpose of such a lock is to ensure that data modification
is executed properly by preventing another transaction from acquiring a lock on the next in-hierarchy
object. It will not allow other transactions to acquire the exclusive lock on that table. There are
six different types of intent locks. Schema Modification (Sch-M) for DDL will be acquired when a DDL
statement is executed, and it will prevent access to the locked object data as the structure of the
object is being changed. Schema Stability (Sch-S) for DML will be acquired when a schema-dependent 
query is being compiled and executed and an execution plan is being generated. This particular lock 
will not block other transactions from accessing the object data, and it is compatible with all locks
except the schema modification lock. Bulk Update (BU) is designed to be used by bulk import 
operations. When acquired, other processes will not be able to access a table during the bulk load
execution. However, a bulk update lock will not prevent another bulk load to be processed in parallel.

    Ironically, one of the most frequently used query hints is `WITH(NOLOCK)`. It is similar to the 
Read Uncommitted isolation level, and is for when one wants to read data without caring too much
about the absolute accuracy of the data. A deadlock occurs during concurrent transactions when two or
more transactions are waiting for each other, but are also blocking each other through locks. This is
resolved automatically: The system will let the more expensive transaction go through and rollback the
less expensive transaction (the "victim").
*/