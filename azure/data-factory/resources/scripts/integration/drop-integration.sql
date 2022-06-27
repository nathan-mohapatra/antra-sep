DROP PROCEDURE IF EXISTS [dbo].[usp_drop_integration_tables];

GO

CREATE PROCEDURE [dbo].[usp_drop_integration_tables]
AS
BEGIN
	DROP TABLE IF EXISTS [dbo].[orders_table];
	DROP TABLE IF EXISTS [dbo].[order_lines_table];
	DROP TABLE IF EXISTS [Fact].[Orders];
	DROP TABLE IF EXISTS [Dimension].[Customers];
END