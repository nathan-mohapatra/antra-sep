DROP PROCEDURE IF EXISTS [dbo].[usp_write_watermark];

GO

CREATE PROCEDURE [dbo].[usp_write_watermark] @LastModifiedtime DATETIME, @TableName VARCHAR(50)
AS
BEGIN
    UPDATE watermarktable
    SET [WatermarkValue] = @LastModifiedtime 
    WHERE [TableName] = @TableName;
END