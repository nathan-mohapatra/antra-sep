DROP TABLE IF EXISTS [dbo].[order_lines_table];

CREATE TABLE [dbo].[order_lines_table] (
	OrderLineID INT,
	OrderID INT,
	StockItemID INT,
	Description NVARCHAR(100),
	PackageTypeID INT,
	Quantity INT,
	UnitPrice DECIMAL(18,2),
	TaxRate DECIMAL(18,3),
	PickedQuantity INT,
	PickingCompletedWhen DATETIME2,
	LastEditedBy INT,
	LastEditedWhen DATETIME2
);

INSERT INTO [dbo].[order_lines_table]
SELECT *
FROM Sales.OrderLines;

DROP PROCEDURE IF EXISTS [dbo].[usp_upsert_order_lines_table];

DROP TYPE IF EXISTS [dbo].[DataTypeforOrderLinesTable];

CREATE TYPE [dbo].[DataTypeforOrderLinesTable] AS TABLE(
	OrderLineID INT,
	OrderID INT,
	StockItemID INT,
	Description NVARCHAR(100),
	PackageTypeID INT,
	Quantity INT,
	UnitPrice DECIMAL(18,2),
	TaxRate DECIMAL(18,3),
	PickedQuantity INT,
	PickingCompletedWhen DATETIME2,
	LastEditedBy INT,
	LastEditedWhen DATETIME2
);

GO

CREATE PROCEDURE [dbo].[usp_upsert_order_lines_table] @order_lines_table [dbo].[DataTypeforOrderLinesTable] READONLY
AS

BEGIN
  MERGE [dbo].[order_lines_table] AS target
  USING @order_lines_table AS source
  ON (target.OrderLineID = source.OrderLineID)
  WHEN MATCHED THEN
      UPDATE SET OrderID = source.OrderID, StockItemID = source.StockItemID, Description = source.Description, PackageTypeID = source.PackageTypeID, Quantity = source.Quantity, UnitPrice = source.UnitPrice, TaxRate = source.TaxRate, PickedQuantity = source.PickedQuantity, 
	  PickingCompletedWhen = source.PickingCompletedWhen, LastEditedBy = source.LastEditedBy, LastEditedWhen = source.LastEditedWhen
  WHEN NOT MATCHED THEN
      INSERT (OrderLineID, OrderID, StockItemID, Description, PackageTypeID, Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, LastEditedBy, LastEditedWhen)
      VALUES (source.OrderLineID, source.OrderID, source.StockItemID, source.Description, source.PackageTypeID, source.Quantity, source.UnitPrice, source.TaxRate, source.PickedQuantity, source.PickingCompletedWhen, source.LastEditedBy, source.LastEditedWhen);
END