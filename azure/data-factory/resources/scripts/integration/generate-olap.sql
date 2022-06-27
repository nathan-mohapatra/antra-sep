DROP PROCEDURE IF EXISTS [dbo].[usp_generate_olap_tables];

GO

CREATE PROCEDURE [dbo].[usp_generate_olap_tables]
AS
BEGIN
	CREATE TABLE [Fact].[Orders] (
		OrderKey INT IDENTITY (1, 1) NOT NULL,
		CustomerKey INT NOT NULL,
		StockItemKey INT NOT NULL,
		OrderDateKey DATE NOT NULL,
		PickedDateKey DATE NULL,
		SalespersonKey INT NOT NULL,
		PickerKey INT NULL,
		WWIOrderID INT NOT NULL,
		WWIBackorderID INT NULL,
		Description NVARCHAR(100) NOT NULL,
		Quantity INT NOT NULL,
		UnitPrice DECIMAL(18,2) NOT NULL,
		TaxRate DECIMAL(18,3) NOT NULL,
		TotalExcludingTax DECIMAL(18,2) NOT NULL,
		TaxAmount DECIMAL(18,2) NOT NULL,
		TotalIncludingTax DECIMAL(18,2) NOT NULL,
		CreatedAt DATETIME2 NOT NULL
	);

	INSERT INTO [Fact].[Orders]
		(CustomerKey, StockItemKey, OrderDateKey, PickedDateKey, SalespersonKey, PickerKey, WWIOrderID, WWIBackorderID, 
		Description, Quantity, UnitPrice, TaxRate, TotalExcludingTax, TaxAmount, TotalIncludingTax, CreatedAt)
	SELECT
		O.CustomerID, OL.StockItemID, O.OrderDate, CAST(O.PickingCompletedWhen AS DATE), O.SalespersonPersonID,
		O.PickedByPersonID, O.OrderID, O.BackorderOrderID, OL.Description, OL.Quantity, OL.UnitPrice, OL.TaxRate,
		OL.Quantity * OL.UnitPrice, OL.Quantity * OL.UnitPrice * OL.TaxRate,
		OL.Quantity * OL.UnitPrice + OL.Quantity * OL.UnitPrice * OL.TaxRate, O.LastEditedWhen
	FROM 
		[dbo].[orders_table] O JOIN [dbo].[order_lines_table] OL ON O.OrderID = OL.OrderID;

	CREATE TABLE [Dimension].[Customers] (
		CustomerKey INT NOT NULL,
		ValidFrom DATETIME2 NOT NULL,
		ValidTo DATETIME2 NULL,
		Active BIT NOT NULL
	);

	INSERT INTO [Dimension].[Customers]
		(CustomerKey, ValidFrom, ValidTo, Active)
	SELECT 
		CustomerKey, CreatedAt AS ValidFrom,
		LEAD(CreatedAt) OVER (PARTITION BY OrderKey ORDER BY CreatedAt) AS ValidTo,
		CASE WHEN LEAD(CreatedAt) OVER (PARTITION BY OrderKey ORDER BY CreatedAt) IS NULL THEN 1 ELSE 0 END
	FROM [Fact].[Orders];
END