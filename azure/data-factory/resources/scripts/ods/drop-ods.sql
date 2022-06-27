DROP PROCEDURE IF EXISTS [dbo].[usp_drop_ods_tables];

GO

CREATE PROCEDURE [dbo].[usp_drop_ods_tables]
AS
BEGIN
	DROP TABLE IF EXISTS [dbo].[orders_table];
	DROP TABLE IF EXISTS [dbo].[order_lines_table];
END