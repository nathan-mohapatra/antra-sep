DROP TABLE IF EXISTS [dbo].[orders_table];

CREATE TABLE [dbo].[orders_table] (
	OrderID INT,
	CustomerID INT,
	SalespersonPersonID INT,
	PickedByPersonID INT,
	ContactPersonID INT,
	BackorderOrderID INT,
	OrderDate DATE,
	ExpectedDeliveryDate DATE,
	CustomerPurchaseOrderNumber NVARCHAR(20),
	IsUndersupplyBackordered BIT,
	Comments NVARCHAR(MAX),
	DeliveryInstructions NVARCHAR(MAX),
	InternalComments NVARCHAR(MAX),
	PickingCompletedWhen DATETIME2,
	LastEditedBy INT,
	LastEditedWhen NVARCHAR(MAX)
);

INSERT INTO [dbo].[orders_table]
SELECT *
FROM Sales.Orders;

DROP PROCEDURE IF EXISTS [dbo].[usp_upsert_orders_table];

DROP TYPE IF EXISTS [dbo].[DataTypeforOrdersTable];

CREATE TYPE [dbo].[DataTypeforOrdersTable] AS TABLE(
	OrderID INT,
	CustomerID INT,
	SalespersonPersonID INT,
	PickedByPersonID INT,
	ContactPersonID INT,
	BackorderOrderID INT,
	OrderDate DATE,
	ExpectedDeliveryDate DATE,
	CustomerPurchaseOrderNumber NVARCHAR(20),
	IsUndersupplyBackordered BIT,
	Comments NVARCHAR(MAX),
	DeliveryInstructions NVARCHAR(MAX),
	InternalComments NVARCHAR(MAX),
	PickingCompletedWhen DATETIME2,
	LastEditedBy INT,
	LastEditedWhen NVARCHAR(MAX)
);

GO

CREATE PROCEDURE [dbo].[usp_upsert_orders_table] @orders_table [dbo].[DataTypeforOrdersTable] READONLY
AS

BEGIN
  MERGE [dbo].[orders_table] AS target
  USING @orders_table AS source
  ON (target.OrderID = source.OrderID)
  WHEN MATCHED THEN
      UPDATE SET CustomerID = source.CustomerID, SalespersonPersonID = source.SalespersonPersonID, PickedByPersonID = source.PickedByPersonID, ContactPersonID = source.ContactPersonID, BackorderOrderID = source.BackorderOrderID, OrderDate = source.OrderDate, 
	  ExpectedDeliveryDate = source.ExpectedDeliveryDate, CustomerPurchaseOrderNumber = source.CustomerPurchaseOrderNumber, IsUndersupplyBackordered = source.IsUndersupplyBackordered, Comments = source.Comments, DeliveryInstructions = source.DeliveryInstructions, 
	  InternalComments = source.InternalComments, PickingCompletedWhen = source.PickingCompletedWhen, LastEditedBy = source.LastEditedBy, LastEditedWhen = source.LastEditedWhen
  WHEN NOT MATCHED THEN
      INSERT (OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen, LastEditedBy, 
	  LastEditedWhen)
      VALUES (source.OrderID, source.CustomerID, source.SalespersonPersonID, source.PickedByPersonID, source.ContactPersonID, source.BackorderOrderID, source.OrderDate, source.ExpectedDeliveryDate, source.CustomerPurchaseOrderNumber, source.IsUndersupplyBackordered, source.Comments, 
	  source.DeliveryInstructions, source.InternalComments, source.PickingCompletedWhen, source.LastEditedBy, source.LastEditedWhen);
END