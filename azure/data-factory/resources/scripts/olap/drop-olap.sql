DROP PROCEDURE IF EXISTS [dbo].[usp_drop_olap_tables];

GO

CREATE PROCEDURE [dbo].[usp_drop_olap_tables]
AS
BEGIN
	DROP TABLE IF EXISTS [Fact].[Orders];
	DROP TABLE IF EXISTS [Dimension].[Customers];
END